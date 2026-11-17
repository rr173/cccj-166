"""Independent reference for the declared constraint semantics.

Nothing here imports the product package.  Explicit steps are executed by
``reference_interpreter`` (also product-free); the *relational* part is a
deliberately naive, independent implementation:

* deletion roots are computed by set difference of row identities;
* the cascade closure is a plain repeat-until-stable loop that re-scans every
  row on every pass (no queue, no ordering, no shared helper);
* validation enumerates every child row and every unique-key group directly.

It is used by the drills/tests to compute expected deletion sets and final row
sets from the small fixtures, so the expectation is never produced by the
production closure function.
"""

import copy
import json


def _canon(v):
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _vt(v):
    if v is None:
        return "null"
    if type(v) is bool:
        return "bool"
    if type(v) is int:
        return "int"
    if type(v) is str:
        return "text"
    raise TypeError(type(v))


def _get(row, field):
    return (row or {}).get(field)


def _fk_value(row, field):
    v = _get(row, field)
    if v is None:
        return None
    if type(v) is int:
        return v
    return "!type!" + str(v)


def cascade_closure(pre, cand, rules):
    """Return the sorted set of (table, row_id) removed by cascade.

    Independent implementation: naive repeated full scans.
    """
    deleted = set()
    for table in set(pre) | set(cand):
        for rid in set(pre.get(table, {})) - set(cand.get(table, {})):
            deleted.add((table, rid))
    changed = True
    while changed:
        changed = False
        for rule in rules:
            if rule["kind"] != "foreign_key" or rule["on_delete"] != "CASCADE":
                continue
            for crid, crow in cand.get(rule["child"], {}).items():
                if (rule["child"], crid) in deleted:
                    continue
                v = _fk_value(crow, rule["field"])
                if v is None or isinstance(v, str):
                    continue
                if (rule["parent"], v) in deleted:
                    deleted.add((rule["child"], crid))
                    changed = True
    return deleted


def violations(cand, rules, deleted=()):
    """Return the list of surviving-state violations (independent scan)."""
    deleted = set(deleted)
    out = []
    for rule in rules:
        if rule["kind"] == "foreign_key":
            for crid, crow in sorted(cand.get(rule["child"], {}).items()):
                v = _fk_value(crow, rule["field"])
                if v is None:
                    continue
                if isinstance(v, str):
                    out.append({"rule": rule["name"], "table": rule["child"],
                                "row_id": crid, "code": "fk_type"})
                    continue
                if v not in cand.get(rule["parent"], {}):
                    policy = rule["on_delete"]
                    code = ("restrict_violation"
                            if policy == "RESTRICT"
                            and (rule["parent"], v) in deleted
                            else "fk_missing")
                    out.append({"rule": rule["name"], "table": rule["child"],
                                "row_id": crid, "code": code,
                                "parent": rule["parent"],
                                "referenced_row_id": v})
    for rule in rules:
        if rule["kind"] != "unique":
            continue
        table = rule["table"]
        groups = {}
        for rid, row in cand.get(table, {}).items():
            token = []
            skipped = False
            for f in rule["fields"]:
                v = _get(row, f)
                if v is None:
                    skipped = True
                    break
                token.append((_vt(v), v))
            if skipped:
                continue
            groups.setdefault(tuple(token), []).append(rid)
        for token, ids in groups.items():
            if len(ids) > 1:
                out.append({"rule": rule["name"], "table": table,
                            "row_ids": sorted(ids), "code": "unique_violation"})
    return sorted(out, key=_canon)


def run_batch(program, rows, rules):
    """Run one batch with constraint checking.  Returns (rows, info).

    `rows` = {table: {row_id: dict}}.  A violating batch leaves `rows`
    unchanged and reports abort code ``constraint_violation`` plus the
    independent reasons and the would-be cascade deletion set.
    """
    import reference_interpreter as ref

    pre = {t: {r: dict(v) for r, v in m.items()} for t, m in rows.items()}
    cand, info = ref.run_batch(program, rows)
    if info["abort"] is not None:
        return pre, info
    deleted = cascade_closure(pre, cand, rules)
    for table, rid in deleted:
        cand[table].pop(rid, None)
    bad = violations(cand, rules, deleted)
    if bad:
        return pre, {"abort": {"code": "constraint_violation", "reasons": bad},
                     "reads": info["reads"], "writes": info["writes"],
                     "guards": info["guards"], "deleted": sorted(deleted)}
    return cand, {"abort": None, "reads": info["reads"], "writes": info["writes"],
                  "guards": info["guards"], "deleted": sorted(deleted)}


def replay(initial_rows, batches, rules, target_ord):
    """Full replay from the seed, skipping `target_ord` (no skip optimisation).

    `batches` is the ordered list of ledger batch records as plain dicts with
    logical_batch_id, batch_ord, status, program.
    """
    rows = copy.deepcopy(initial_rows)
    outcomes = {}
    deletions = {}
    for b in batches:
        bo = b["batch_ord"]
        if bo == target_ord:
            outcomes[bo] = {"status": "Superseded"}
            continue
        if b["status"] == "failed":
            outcomes[bo] = {"status": "Blocked", "code": "historical_failure"}
            continue
        pre = {t: {r: dict(v) for r, v in m.items()} for t, m in rows.items()}
        rows, info = run_batch(b["program"], rows, rules)
        if info["abort"] is not None:
            rows = pre
            outcomes[bo] = {"status": "Blocked",
                            "code": info["abort"]["code"]}
            deletions[bo] = []
        else:
            outcomes[bo] = {"status": "Ran"}
            deletions[bo] = info["deleted"]
    return rows, outcomes, deletions


def snapshot_rows(ledger_snapshots):
    return {t: {rid: dict(r) for rid, r in m.items()}
            for t, m in ledger_snapshots.items()}
