"""Independent reference interpreter for the public batch definition.

This module intentionally imports nothing from the product package.  It reads
the same validated program data and replays *from the initial state*, skipping
the revoked target, executing every other successful batch in original order.
It is the small-sample oracle the product output is checked against.

It is deliberately conservative: it has no skip optimisation at all.  Every
later successful batch is fully replayed.  If the product reports a batch as
Kept/Skipped or Changed, this oracle must agree value by value per table.
"""

import json


def _canon(v):
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _resolve(env, ref):
    if "." in ref:
        alias, _, col = ref.partition(".")
        e = env["reads"].get(alias)
        if e is None:
            raise KeyError(alias)
        row = e["by_id"].get(env["rid"])
        return None if row is None else row.get(col)
    return env["row"].get(ref)


def _pred(node, env):
    k = node["kind"]
    if k == "all":
        return True
    if k == "and":
        return all(_pred(a, env) for a in node["args"])
    if k == "or":
        return any(_pred(a, env) for a in node["args"])
    if k == "not":
        return not _pred(node["arg"], env)
    v = _resolve(env, node["field"])
    if k == "eq":
        c = node["value"]
        return v is not None and type(v) is type(c) and v == c
    if k == "range":
        return type(v) is int and node["lo"] <= v <= node["hi"]
    if k == "is_null":
        return (v is None) == node["value"]
    raise ValueError(k)


def _value(expr, env):
    keys = set(expr)
    if keys == {"const"}:
        return expr["const"]
    if keys == {"field"}:
        return _resolve(env, expr["field"])
    op = next(iter(keys))
    a, b = expr[op]
    x, y = _value(a, env), _value(b, env)
    if type(x) is not int or type(y) is not int:
        raise TypeError("int only")
    return x + y if op == "add" else x - y


def _rid(expr):
    if "const" in expr:
        return expr["const"]
    return expr["add"][0]["const"] + expr["add"][1]


class Abort(Exception):
    def __init__(self, code, step, detail=None):
        super().__init__(code)
        self.code = code
        self.step = step
        self.detail = detail or {}


def run_batch(program, rows):
    """Run one batch on `rows` = {table: {row_id: dict}}.  Returns (new_rows, info).

    On a business failure returns the unchanged rows copy and info with a
    non-None `abort`; the caller treats that decision as failed/blocked.
    """
    pre = {t: {r: dict(v) for r, v in m.items()} for t, m in rows.items()}
    rows = {t: {r: dict(v) for r, v in m.items()} for t, m in pre.items()}
    reads = {}
    def fail(code, step, detail=None):
        # Atomic batch: a failing batch leaves no writes at all.
        return pre, {"abort": {"code": code, "step": step, "detail": detail or {}},
                     "reads": read_ev, "writes": write_ev, "guards": guard_ev}
    read_ev = []
    write_ev = []
    guard_ev = []
    for i, st in enumerate(program["steps"]):
        op = st["op"]
        if op == "read":
            tbl = st["select"]["table"]
            table_rows = rows.setdefault(tbl, {})
            sel = {}
            cls = []
            for ent in st["map"]:
                rid = ent["row_id"]
                row = table_rows.get(rid)
                if row is None:
                    cls.append({"row_id": rid, "status": "absent"})
                    continue
                env = {"row": row, "rid": rid, "reads": reads}
                keep = _pred(st["select"]["where"], env)
                cls.append({"row_id": rid, "status": "selected" if keep else "excluded"})
                if keep:
                    sel[rid] = dict(row)
            ids = sorted(sel)
            reads[st["alias"]] = {"table": tbl, "by_id": sel, "ids": ids}
            read_ev.append({"step_index": i, "alias": st["alias"], "table": tbl,
                            "selected_ids": ids, "classification": cls})
        elif op == "guard":
            e = reads[st["alias"]]
            if st["test"] == "row_count":
                value = len(e["ids"])
            else:
                total = 0
                for rid in e["ids"]:
                    v = e["by_id"][rid].get(st["field"])
                    if v is not None:
                        if type(v) is not int:
                            return fail("type_error", i)
                        total += v
                value = total
            if "eq" in st and value != st["eq"]:
                guard_ev.append({"step_index": i, "value": value, "passed": False})
                return fail("guard_failed", i)
            if "ge" in st and value < st["ge"]:
                guard_ev.append({"step_index": i, "value": value, "passed": False})
                return fail("guard_failed", i)
            if "le" in st and value > st["le"]:
                guard_ev.append({"step_index": i, "value": value, "passed": False})
                return fail("guard_failed", i)
            guard_ev.append({"step_index": i, "value": value, "passed": True})
        elif op == "write":
            e = reads[st["rows_alias"]]
            tbl = st["table"]
            table_rows = rows.setdefault(tbl, {})
            effect = st["effect"]
            targets = []
            for ent in st["rows"]:
                src = ent["source_row_id"]
                if src not in e["ids"]:
                    return fail("write_source_missing", i)
                targets.append(_rid(ent["row_id"]))
            if effect in ("insert", "replace") and len(set(targets)) != len(targets):
                return fail("pk_conflict", i)
            if effect == "insert" and any(t in table_rows for t in targets):
                return fail("pk_conflict", i)
            if effect == "delete" and any(t not in table_rows for t in targets):
                return fail("delete_missing", i)
            step_ev = []
            for ent, tgt in zip(st["rows"], targets):
                if effect == "delete":
                    del table_rows[tgt]
                    step_ev.append({"target": tgt, "effect": "delete"})
                else:
                    src = ent["source_row_id"]
                    env = {"row": e["by_id"][src], "rid": src, "reads": reads}
                    row = {}
                    try:
                        for col, expr in ent.get("values", {}).items():
                            row[col] = _value(expr, env)
                    except TypeError as exc:
                        return fail("type_error", i, str(exc))
                    table_rows[tgt] = row
                    step_ev.append({"target": tgt, "row": dict(row)})
            write_ev.append({"step_index": i, "table": tbl, "effect": effect,
                             "rows": step_ev})
    return rows, {"abort": None, "reads": read_ev, "writes": write_ev,
                  "guards": guard_ev}
