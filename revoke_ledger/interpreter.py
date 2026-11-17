"""Deterministic execution of validated programs against a managed state.

The interpreter is the only place that turns program data into effects.  It
operates on an explicit row map; no SQL is ever built from program data.
"""

from .canon import canon, sha256
from .errors import BatchAbort

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


def value_type(v):
    if v is None:
        return "null"
    if type(v) is bool:
        return "bool"
    if type(v) is int:
        return "int"
    if type(v) is str:
        return "text"
    raise TypeError(f"unsupported value type {type(v).__name__}")


def normalize_field_value(v, step_index, col):
    try:
        value_type(v)
    except TypeError as exc:
        raise BatchAbort("type_error", f"step {step_index}: column {col!r}: {exc}",
                         step_index=step_index)
    if type(v) is int and not (INT64_MIN <= v <= INT64_MAX):
        raise BatchAbort("type_error", f"step {step_index}: column {col!r} int out of int64",
                         step_index=step_index)
    return v


def _resolve(env, ref):
    if "." in ref:
        alias, _, col = ref.partition(".")
        entry = env["reads"].get(alias)
        if entry is None:
            raise BatchAbort("internal_error", f"unknown alias {alias!r}")
        row = entry["rows_by_id"].get(env["row_id"])
        return None if row is None else row.get(col)
    return env["row"].get(ref)


def eval_value(expr, env, step_index):
    keys = set(expr)
    if keys == {"const"}:
        v = expr["const"]
        return normalize_field_value(v, step_index, "const")
    if keys == {"field"}:
        return _resolve(env, expr["field"])
    op = next(iter(keys))
    a, b = expr[op]
    va = eval_value(a, env, step_index)
    vb = eval_value(b, env, step_index)
    if type(va) is not int or type(vb) is not int:
        raise BatchAbort("type_error",
                         f"step {step_index}: {op} needs integer operands, got "
                         f"{value_type(va)}/{value_type(vb)}", step_index=step_index)
    r = va + vb if op == "add" else va - vb
    if not (INT64_MIN <= r <= INT64_MAX):
        raise BatchAbort("type_error", f"step {step_index}: integer overflow in {op}",
                         step_index=step_index)
    return r


def eval_pred(node, env):
    kind = node["kind"]
    if kind == "all":
        return True
    if kind == "and":
        return all(eval_pred(a, env) for a in node["args"])
    if kind == "or":
        return any(eval_pred(a, env) for a in node["args"])
    if kind == "not":
        return not eval_pred(node["arg"], env)
    v = _resolve(env, node["field"])
    if kind == "eq":
        c = node["value"]
        return v is not None and value_type(v) == value_type(c) and v == c
    if kind == "range":
        return type(v) is int and node["lo"] <= v <= node["hi"]
    if kind == "is_null":
        return (v is None) == node["value"]
    raise BatchAbort("internal_error", f"unknown predicate {kind!r}")


def eval_id_expr(expr):
    if "const" in expr:
        return expr["const"]
    base, delta = expr["add"]
    return base["const"] + delta


def state_snapshot(state):
    return [[t, rid, canon(state.get_row(t, rid))]
            for t in state.managed_tables() for rid in state.row_ids(t)]


def state_hash(state):
    return sha256({"tables": state_snapshot(state)})


def row_digest(row):
    return sha256(row) if row is not None else None


def finalize_constraints(constraints, pre_state, state, writes_ev, step_count):
    """Shared batch-final constraint gate for every program schema.

    Runs the declared rules on the completed candidate state (cascade closure,
    then surviving-row checks).  Returns (report, abort-or-None).
    """
    report = constraints.finalize(pre_state, state, writes_ev)
    if report["ok"]:
        return report, None
    first = report["reasons"][0]
    where = f"{first['table']}[{first.get('row_id')}]"
    target = (" no rows are written"
              if not report["deleted"] else
              f" its {len(report['deleted'])} planned deletes are discarded")
    abort = BatchAbort(
        "constraint_violation",
        f"{where} violates rule {first['rule']!r} ({first['code']});"
        f"{target}", step_index=step_count, table=first["table"],
        row_id=first.get("row_id"),
        detail={"constraints": report, "reasons": report["reasons"]})
    return report, abort


def execute_program(prog, state, constraints=None):
    """Execute one validated program on `state`.

    Dispatches on the program schema: managed-batch/v1 programs run the
    explicit row-mapping interpreter below; managed-relational/v1 programs
    run the relational pipeline in relational.py.  Both share the same
    batch-final constraint gate and the same evidence contract.

    Mutates `state` as it goes.  On failure the caller must discard the state.
    Returns evidence:
      ok, reads, writes, guards, read_footprint, write_footprint,
      read_contents, action_taken, abort, constraints.

    When `constraints` is a ConstraintSet, explicit steps run unimpeded and see
    their own transient writes.  Only after every explicit step and guard has
    succeeded is the completed candidate state constrained: cascade deletes are
    applied, then surviving references and unique keys are checked.  A failure
    raises BatchAbort('constraint_violation') with the full report in detail.
    """
    if prog.get("schema") == "managed-relational/v1":
        from .relational import execute_relational_program
        return execute_relational_program(prog, state, constraints=constraints)
    if prog.get("schema") == "managed-cdc/v1":
        from .cdcexec import execute_cdc_program
        return execute_cdc_program(prog, state, constraints=constraints)
    pre_state = state.copy() if constraints is not None else None
    reads_ev = []
    writes_ev = []
    guards_ev = []
    aborted = None
    reads_env = {}
    read_footprint = {}
    write_footprint = {}
    read_contents = {}
    action_taken = False

    for idx, st in enumerate(prog["steps"]):
        if aborted is not None:
            break
        op = st["op"]
        if op == "read":
            table = st["select"]["table"]
            where = st["select"]["where"]
            selected = []
            classification = []
            selected_rows = {}
            for ent in st["map"]:
                rid = ent["row_id"]
                row = state.get_row(table, rid)
                if row is None:
                    classification.append({"row_id": rid, "status": "absent"})
                    continue
                env = {"row": row, "row_id": rid, "reads": reads_env}
                keep = eval_pred(where, env)
                classification.append({
                    "row_id": rid,
                    "status": "selected" if keep else "excluded",
                    "row": canon(row),
                })
                if keep:
                    selected.append(rid)
                    selected_rows[rid] = row
                    read_contents.setdefault(table, {})[rid] = row
                    read_footprint.setdefault(table, set()).add(rid)
            selected.sort()
            reads_ev.append({
                "step_index": idx,
                "alias": st["alias"],
                "table": table,
                "where": where,
                "map": st["map"],
                "map_size": len(st["map"]),
                "candidate_count": sum(1 for c in classification if c["status"] != "absent"),
                "selected_ids": selected,
                "selected_count": len(selected),
                "selected_digest": sha256([[r, canon(selected_rows[r])] for r in selected]),
                "classification": classification,
            })
            reads_env[st["alias"]] = {
                "table": table,
                "selected_ids": selected,
                "rows_by_id": selected_rows,
                "step_index": idx,
            }
            continue

        if op == "guard":
            entry = reads_env[st["alias"]]
            if st["test"] == "row_count":
                value = len(entry["selected_ids"])
            else:
                field = st["field"]
                total = 0
                for rid in entry["selected_ids"]:
                    v = entry["rows_by_id"][rid].get(field)
                    if v is None:
                        continue
                    if type(v) is not int:
                        aborted = BatchAbort(
                            "type_error",
                            f"step {idx}: int_sum field {field!r} is {value_type(v)}, not int",
                            step_index=idx, table=entry["table"], row_id=rid)
                        break
                    total += v
                if aborted is not None:
                    break
                value = total
            bounds = {k: st[k] for k in ("eq", "ge", "le") if k in st}
            reasons = []
            if "eq" in bounds and value != bounds["eq"]:
                reasons.append(f"value {value} != {bounds['eq']}")
            if "ge" in bounds and value < bounds["ge"]:
                reasons.append(f"value {value} < {bounds['ge']}")
            if "le" in bounds and value > bounds["le"]:
                reasons.append(f"value {value} > {bounds['le']}")
            passed = not reasons
            guards_ev.append({"step_index": idx, "test": st["test"],
                              "alias": st["alias"], "field": st.get("field"),
                              "value": value, "bounds": bounds, "passed": passed,
                              "reasons": reasons})
            if not passed:
                aborted = BatchAbort(
                    "guard_failed",
                    f"step {idx}: guard {st['test']} on {st['alias']!r} = {value} "
                    f"fails {bounds}", step_index=idx, table=entry["table"])
            continue

        if op == "write":
            alias = st["rows_alias"]
            entry = reads_env[alias]
            effect = st["effect"]
            table = st["table"]
            targets = []
            for ent in st["rows"]:
                src = ent["source_row_id"]
                if src not in entry["selected_ids"]:
                    aborted = BatchAbort(
                        "write_source_missing",
                        f"step {idx}: source row {src} was not selected by read "
                        f"{alias!r}; a successful write requires its source read",
                        step_index=idx, table=table, row_id=src)
                    break
                targets.append(eval_id_expr(ent["row_id"]))
            if aborted is not None:
                break
            if effect in ("insert", "replace") and len(set(targets)) != len(targets):
                seen, dup = set(), set()
                for t in targets:
                    (dup if t in seen else seen).add(t)
                aborted = BatchAbort("pk_conflict",
                                     f"step {idx}: duplicate target row_id(s) {sorted(dup)} "
                                     f"within one write", step_index=idx, table=table)
                break
            if effect == "insert":
                clash = [t for t in targets if state.get_row(table, t) is not None]
                if clash:
                    aborted = BatchAbort(
                        "pk_conflict",
                        f"step {idx}: insert target row_id {clash[0]} already exists",
                        step_index=idx, table=table, row_id=clash[0])
                    break
            if effect == "delete":
                missing = [t for t in targets if state.get_row(table, t) is None]
                if missing:
                    aborted = BatchAbort(
                        "delete_missing",
                        f"step {idx}: delete target row_id {missing[0]} is absent",
                        step_index=idx, table=table, row_id=missing[0])
                    break
            step_rows = []
            for ent, tgt in zip(st["rows"], targets):
                src = ent["source_row_id"]
                before = state.get_row(table, tgt)
                row_ev = {"source_row_id": src, "target_row_id": tgt,
                          "before": row_digest(before)}
                if effect == "delete":
                    state.delete_row(table, tgt)
                    after = None
                else:
                    values = ent.get("values", {})
                    new_row = {}
                    for col, expr in values.items():
                        new_row[col] = normalize_field_value(
                            eval_value(expr, {"row": entry["rows_by_id"][src],
                                              "row_id": src, "reads": reads_env},
                                       idx), idx, col)
                    state.put_row(table, tgt, new_row)
                    after = new_row
                row_ev["after"] = row_digest(after)
                action_taken = True
                step_rows.append(row_ev)
                write_footprint.setdefault(table, set()).add(tgt)
            writes_ev.append({"step_index": idx, "effect": effect, "table": table,
                              "rows_alias": alias, "rows": step_rows})
            continue

        aborted = BatchAbort("internal_error", f"step {idx}: unknown op {op!r}",
                             step_index=idx)

    report = None
    if aborted is None and constraints is not None:
        report, aborted = finalize_constraints(
            constraints, pre_state, state, writes_ev, len(prog["steps"]))

    return {
        "ok": aborted is None,
        "reads": reads_ev,
        "writes": writes_ev,
        "guards": guards_ev,
        "read_footprint": {t: sorted(s) for t, s in sorted(read_footprint.items())},
        "write_footprint": {t: sorted(s) for t, s in sorted(write_footprint.items())},
        "read_contents": {t: {str(k): read_contents[t][k]
                              for k in sorted(read_contents[t])}
                          for t in sorted(read_contents)},
        "action_taken": action_taken,
        "constraints": report,
        "abort": aborted.as_dict() if aborted is not None else None,
    }
