"""Deterministic execution of managed-relational/v1 programs.

A relational program forms immutable relation aliases from explicit read maps
(snapshots: later writes in the same batch never change an already-formed
relation), then joins, projects, groups and writes derived rows.  Every
operator is a pure, deterministic function of its input relations; nothing
here executes caller code, SQL or external requests.

Row-occurrence identity
-----------------------
Every row occurrence in a relation carries a stable identity that depends
only on the program and the read *results* (never on the caller's map order):

  read:    ["src", <step>, <row_id>]
  join:    ["join", <step>, <left identity>, <right identity|null>]
  project: ["proj", <step>, <source identity>]
  group:   ["grp", <step>, [<typed key component>, ...]]

Two input rows with identical values stay distinct (different source
row_ids), and different join pairings of the same rows stay distinct
(different pair identities).  Relations are kept sorted by the canonical
form of their identities, so permuting an input map changes neither the
identities nor any downstream result.

Join semantics
--------------
* Keys compare by type AND value: int 1, bool true and text "1" are three
  different keys.  A null or missing key component (on either side) never
  matches -- not even null vs null.
* Duplicate contributions are preserved: two left rows and three right rows
  sharing a key produce six inner results, never a value-deduplicated set.
* A left join keeps every unmatched left row once; its right-side fields are
  null.  Joined relations can be joined or grouped again (no fixed two-table
  template): a join's fields are "left.<f>"/"right.<f>" of its inputs.

Grouping semantics
------------------
* Group keys are typed (1/true/"1" differ) but null and a missing field are
  the same empty component -- deliberately different from join keys.
* Aggregates: count_star, count(field) (skips null), sum (non-bool int64
  only, skips null, null when all null), min/max (skips null; the non-null
  values of a group must share one type, int or text).
* sum accumulates in exact integer arithmetic and only the final result is
  range-checked against int64, so a representable result is never rejected
  because of input order (e.g. 2**62 + 2**62 - 2**62 = 2**62).
* Empty input: with keys there are no groups; a keyless global aggregation
  still produces one row with counts 0 and all other aggregates null.
* Type errors and final overflow abort the whole batch with the step, the
  aggregate name and the group key in the diagnostics.

Derived writes
--------------
A write step writes every row of a derived relation.  The target row_id comes
from a caller-declared integer field or the closed safe integer expressions
(const/field/add/sub) -- never from a traversal index or a hash.  Two derived
rows computing the same target row_id in one write step reject the whole
batch, even when their contents are identical.  Guards over derived relations
(row_count / int_sum) fail the batch the same way; earlier writes of the same
batch never survive a later failure.  Post-write unique/foreign-key/cascade
checks are the existing batch-final constraint gate (interpreter.
finalize_constraints) -- there is no separate unconstrained import path.
"""

from .canon import canon, sha256
from .errors import BatchAbort
from .interpreter import (INT64_MAX, INT64_MIN, eval_pred, finalize_constraints,
                          normalize_field_value, row_digest, value_type)

MAX_RELATION_ROWS = 100_000


def _ikey(identity):
    return canon(identity)


class _Relation:
    """An immutable snapshot: sorted (identity, row-dict) occurrences."""

    __slots__ = ("rows",)

    def __init__(self, rows):
        self.rows = rows

    def digest(self):
        return sha256([[canon(i), canon(r)] for i, r in self.rows])


def _join_key(row, fields):
    """Typed equality token for a join key, or None (never matches)."""
    toks = []
    for f in fields:
        v = row.get(f)
        if v is None:
            return None
        toks.append((value_type(v), v))
    return tuple(toks)


def _group_token(v):
    """Typed group-key component; null and missing are the same empty value."""
    if v is None:
        return ("null", None)
    return (value_type(v), v)


def _eval_expr(expr, row, step_index):
    """Closed safe integer/scalar expressions over one derived row."""
    keys = set(expr)
    if keys == {"const"}:
        return expr["const"]
    if keys == {"field"}:
        return row.get(expr["field"])
    op = next(iter(keys))
    a, b = expr[op]
    va = _eval_expr(a, row, step_index)
    vb = _eval_expr(b, row, step_index)
    if type(va) is not int or type(vb) is not int:
        raise BatchAbort("type_error",
                         f"step {step_index}: {op} needs integer operands, got "
                         f"{value_type(va)}/{value_type(vb)}",
                         step_index=step_index)
    r = va + vb if op == "add" else va - vb
    if not (INT64_MIN <= r <= INT64_MAX):
        raise BatchAbort("type_error",
                         f"step {step_index}: integer overflow in {op}",
                         step_index=step_index)
    return r


def _agg_sum(members, field, step_index, agg, group_key):
    total = 0
    seen = False
    for sid, row in members:
        v = row.get(field)
        if v is None:
            continue
        if type(v) is not int:
            raise BatchAbort(
                "type_error",
                f"step {step_index}: aggregate {agg['as']!r} (sum) requires "
                f"non-bool int64 values, got {value_type(v)}",
                step_index=step_index,
                detail={"aggregate": agg["as"], "op": "sum", "field": field,
                        "group_key": group_key, "source_identity": sid,
                        "offending": {"type": value_type(v), "value": v}})
        total += v          # exact: Python ints never overflow mid-accumulation
        seen = True
    if not seen:
        return None
    if not (INT64_MIN <= total <= INT64_MAX):
        raise BatchAbort(
            "type_error",
            f"step {step_index}: aggregate {agg['as']!r} (sum) result {total} "
            f"is out of int64 range",
            step_index=step_index,
            detail={"aggregate": agg["as"], "op": "sum", "field": field,
                    "group_key": group_key, "result": total})
    return total


def _agg_minmax(members, field, want_max, step_index, agg, group_key):
    vals = []
    for sid, row in members:
        v = row.get(field)
        if v is not None:
            vals.append((sid, v))
    if not vals:
        return None
    types = {value_type(v) for _, v in vals}
    if len(types) != 1:
        raise BatchAbort(
            "type_error",
            f"step {step_index}: aggregate {agg['as']!r} "
            f"({'max' if want_max else 'min'}) group mixes types "
            f"{sorted(types)}",
            step_index=step_index,
            detail={"aggregate": agg["as"],
                    "op": "max" if want_max else "min", "field": field,
                    "group_key": group_key, "types": sorted(types)})
    t = next(iter(types))
    if t not in ("int", "text"):
        raise BatchAbort(
            "type_error",
            f"step {step_index}: aggregate {agg['as']!r} "
            f"({'max' if want_max else 'min'}) only accepts int or text "
            f"values, got {t}",
            step_index=step_index,
            detail={"aggregate": agg["as"],
                    "op": "max" if want_max else "min", "field": field,
                    "group_key": group_key, "types": [t]})
    best = vals[0][1]
    for _, v in vals[1:]:
        if (v > best) if want_max else (v < best):
            best = v
    return best


def execute_relational_program(prog, state, constraints=None):
    """Execute one validated managed-relational/v1 program on `state`.

    Same contract as interpreter.execute_program: mutates `state`; on failure
    the caller discards it.  Business aborts are captured in the returned
    evidence (ok=False) with every relation formed before the abort still
    recorded, so failure evidence stays explainable.
    """
    pre_state = state.copy() if constraints is not None else None
    reads_ev = []
    relations_ev = []
    writes_ev = []
    guards_ev = []
    aborted = None
    reads_env = {}       # read alias -> v1-style entry (predicate field refs)
    relations = {}       # alias -> _Relation
    read_footprint = {}
    write_footprint = {}
    read_contents = {}
    action_taken = False

    for idx, st in enumerate(prog["steps"]):
        if aborted is not None:
            break
        op = st["op"]
        try:
            # ------------------------------------------------------ read
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
                rid_field = st.get("row_id_field")
                reads_ev.append({
                    "step_index": idx,
                    "alias": st["alias"],
                    "table": table,
                    "where": where,
                    "map": st["map"],
                    "map_size": len(st["map"]),
                    "candidate_count": sum(1 for c in classification
                                           if c["status"] != "absent"),
                    "selected_ids": selected,
                    "selected_count": len(selected),
                    "selected_digest": sha256(
                        [[r, canon(selected_rows[r])] for r in selected]),
                    "classification": classification,
                    "row_id_field": rid_field,
                })
                reads_env[st["alias"]] = {
                    "table": table,
                    "selected_ids": selected,
                    "rows_by_id": selected_rows,
                    "step_index": idx,
                }
                rows = []
                for rid in selected:
                    r = dict(selected_rows[rid])
                    if rid_field is not None:
                        r[rid_field] = rid
                    rows.append((["src", idx, rid], r))
                rel = _Relation(rows)
                relations[st["alias"]] = rel
                relations_ev.append({
                    "step_index": idx, "op": "read", "alias": st["alias"],
                    "row_count": len(rows), "digest": rel.digest(),
                    "detail": {"table": table, "row_id_field": rid_field,
                               "selected_ids": selected},
                })
                continue

            # ------------------------------------------------------ join
            if op == "join":
                left = relations[st["left"]]
                right = relations[st["right"]]
                lf = [k["left"] for k in st["on"]]
                rf = [k["right"] for k in st["on"]]
                index = {}
                for rid_, rrow in right.rows:
                    tok = _join_key(rrow, rf)
                    if tok is not None:
                        index.setdefault(tok, []).append((rid_, rrow))
                right_fields = sorted({k for _, r in right.rows for k in r})
                out = []
                pairs = []
                unmatched_left = []
                matched_right = set()
                for lid, lrow in left.rows:
                    tok = _join_key(lrow, lf)
                    matches = index.get(tok, ()) if tok is not None else ()
                    if matches:
                        for rid_, rrow in matches:
                            matched_right.add(_ikey(rid_))
                            combined = {"left." + k: v for k, v in lrow.items()}
                            combined.update({"right." + k: v
                                             for k, v in rrow.items()})
                            out.append((["join", idx, lid, rid_], combined))
                            pairs.append({"left": lid, "right": rid_})
                            if len(out) > MAX_RELATION_ROWS:
                                raise BatchAbort(
                                    "limit_exceeded",
                                    f"step {idx}: join result exceeds "
                                    f"{MAX_RELATION_ROWS} rows",
                                    step_index=idx)
                    else:
                        unmatched_left.append(lid)
                        if st["how"] == "left":
                            combined = {"left." + k: v for k, v in lrow.items()}
                            combined.update({"right." + k: None
                                             for k in right_fields})
                            out.append((["join", idx, lid, None], combined))
                out.sort(key=lambda kv: _ikey(kv[0]))
                rel = _Relation(out)
                relations[st["alias"]] = rel
                relations_ev.append({
                    "step_index": idx, "op": "join", "alias": st["alias"],
                    "row_count": len(out), "digest": rel.digest(),
                    "detail": {
                        "left": st["left"], "right": st["right"],
                        "how": st["how"], "on": st["on"],
                        "pairs": pairs,
                        "unmatched_left": unmatched_left,
                        "unmatched_right": sorted(
                            (rid_ for rid_, _ in right.rows
                             if _ikey(rid_) not in matched_right), key=_ikey),
                    },
                })
                continue

            # --------------------------------------------------- project
            if op == "project":
                src = relations[st["from"]]
                out = []
                for sid, srow in src.rows:
                    nrow = {item["as"]: srow.get(item["from"])
                            for item in st["select"]}
                    out.append((["proj", idx, sid], nrow))
                rel = _Relation(out)
                relations[st["alias"]] = rel
                relations_ev.append({
                    "step_index": idx, "op": "project", "alias": st["alias"],
                    "row_count": len(out), "digest": rel.digest(),
                    "detail": {"from": st["from"], "select": st["select"]},
                })
                continue

            # ----------------------------------------------------- group
            if op == "group":
                src = relations[st["from"]]
                keys = st["keys"]
                buckets = {}
                if not keys:
                    buckets[()] = []
                for sid, srow in src.rows:
                    tok = tuple(_group_token(srow.get(k)) for k in keys)
                    buckets.setdefault(tok, []).append((sid, srow))
                out = []
                groups_ev = []
                for tok in sorted(buckets, key=lambda t: canon([list(t)])):
                    members = buckets[tok]
                    nrow = {}
                    for k, comp in zip(keys, tok):
                        nrow[k] = comp[1]
                    gkey = [{"type": t, "value": v} for t, v in tok]
                    gvals = {}
                    for agg in st["aggregates"]:
                        opk = agg["op"]
                        if opk == "count_star":
                            val = len(members)
                        elif opk == "count":
                            val = sum(1 for _, r in members
                                      if r.get(agg["field"]) is not None)
                        elif opk == "sum":
                            val = _agg_sum(members, agg["field"], idx, agg,
                                           gkey)
                        else:
                            val = _agg_minmax(members, agg["field"],
                                              opk == "max", idx, agg, gkey)
                        nrow[agg["as"]] = val
                        gvals[agg["as"]] = val
                    gid = ["grp", idx, [[t, v] for t, v in tok]]
                    out.append((gid, nrow))
                    groups_ev.append({
                        "key": gkey,
                        "identity": gid,
                        "members": [sid for sid, _ in members],
                        "values": gvals,
                    })
                rel = _Relation(out)
                relations[st["alias"]] = rel
                relations_ev.append({
                    "step_index": idx, "op": "group", "alias": st["alias"],
                    "row_count": len(out), "digest": rel.digest(),
                    "detail": {"from": st["from"], "keys": keys,
                               "aggregates": st["aggregates"],
                               "groups": groups_ev},
                })
                continue

            # ----------------------------------------------------- write
            if op == "write":
                rel = relations[st["from"]]
                effect = st["effect"]
                table = st["table"]
                computed = []
                targets = []
                for sid, srow in rel.rows:
                    tgt = _eval_expr(st["target"], srow, idx)
                    # bool is not int here; null/text targets are rejected too.
                    if type(tgt) is not int:
                        raise BatchAbort(
                            "type_error",
                            f"step {idx}: write target row_id must be a "
                            f"non-null int64, got {value_type(tgt)}",
                            step_index=idx, table=table,
                            detail={"source_identity": sid,
                                    "target": {"type": value_type(tgt),
                                               "value": tgt}})
                    vals = {}
                    if effect != "delete":
                        for col, expr in st.get("values", {}).items():
                            vals[col] = normalize_field_value(
                                _eval_expr(expr, srow, idx), idx, col)
                    targets.append(tgt)
                    computed.append((sid, tgt, vals))
                if len(set(targets)) != len(targets):
                    seen, dups = set(), set()
                    for t in targets:
                        (dups if t in seen else seen).add(t)
                    raise BatchAbort(
                        "pk_conflict",
                        f"step {idx}: duplicate target row_id(s) "
                        f"{sorted(dups)} within one write step",
                        step_index=idx, table=table,
                        detail={"duplicates": sorted(dups),
                                "sources": [sid for sid, tgt, _ in computed
                                            if tgt in dups]})
                if effect == "insert":
                    clash = [t for t in targets
                             if state.get_row(table, t) is not None]
                    if clash:
                        raise BatchAbort(
                            "pk_conflict",
                            f"step {idx}: insert target row_id {clash[0]} "
                            f"already exists",
                            step_index=idx, table=table, row_id=clash[0])
                if effect == "delete":
                    missing = [t for t in targets
                               if state.get_row(table, t) is None]
                    if missing:
                        raise BatchAbort(
                            "delete_missing",
                            f"step {idx}: delete target row_id {missing[0]} "
                            f"is absent",
                            step_index=idx, table=table, row_id=missing[0])
                step_rows = []
                for sid, tgt, vals in computed:
                    before = state.get_row(table, tgt)
                    row_ev = {"source_identity": sid, "target_row_id": tgt,
                              "before": row_digest(before)}
                    if effect == "delete":
                        state.delete_row(table, tgt)
                        after = None
                    else:
                        state.put_row(table, tgt, vals)
                        after = vals
                    row_ev["after"] = row_digest(after)
                    action_taken = True
                    step_rows.append(row_ev)
                    write_footprint.setdefault(table, set()).add(tgt)
                writes_ev.append({"step_index": idx, "effect": effect,
                                  "table": table, "from": st["from"],
                                  "rows": step_rows})
                continue

            # ----------------------------------------------------- guard
            if op == "guard":
                rel = relations[st["relation"]]
                if st["test"] == "row_count":
                    value = len(rel.rows)
                else:
                    total = 0
                    for sid, srow in rel.rows:
                        v = srow.get(st["field"])
                        if v is None:
                            continue
                        if type(v) is not int:
                            raise BatchAbort(
                                "type_error",
                                f"step {idx}: int_sum field {st['field']!r} "
                                f"is {value_type(v)}, not int",
                                step_index=idx,
                                detail={"relation": st["relation"],
                                        "source_identity": sid})
                        total += v
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
                                  "relation": st["relation"],
                                  "field": st.get("field"),
                                  "value": value, "bounds": bounds,
                                  "passed": passed, "reasons": reasons})
                if not passed:
                    raise BatchAbort(
                        "guard_failed",
                        f"step {idx}: guard {st['test']} on relation "
                        f"{st['relation']!r} = {value} fails {bounds}",
                        step_index=idx)
                continue

            raise BatchAbort("internal_error", f"step {idx}: unknown op {op!r}",
                             step_index=idx)
        except BatchAbort as exc:
            aborted = exc

    report = None
    if aborted is None and constraints is not None:
        report, aborted = finalize_constraints(
            constraints, pre_state, state, writes_ev, len(prog["steps"]))

    return {
        "ok": aborted is None,
        "schema": "managed-relational/v1",
        "reads": reads_ev,
        "relations": relations_ev,
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
