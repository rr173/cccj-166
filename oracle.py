"""Oracle comparison: product result vs. full independent replay.

For a drill we record the whole pre-revocation run, then ask the reference
interpreter (which imports nothing from the product) to replay the official
initial state, skipping the target, and produce the expected per-table state
and per-batch outcome.  The product's plan and applied generation must agree
value by value.

Usage:
    python3 oracle.py <db-path> <revocation_id> <target_logical_id>

or import and call compare_oracle().
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import reference_interpreter as ref  # noqa: E402


def _initial_state(db):
    """The committed state right after seeding (generation kind 'seed')."""
    for g in db.generations():
        if g["kind"] == "seed":
            st = {}
            for t, rid, row in db.rows_for_generation(g["generation"]):
                st.setdefault(t, {})[rid] = row
            for t in db.tables_for_generation(g["generation"]):
                st.setdefault(t, {})
            return st
    raise RuntimeError("no seed generation")


def _rows_only(state):
    return {t: {rid: dict(row) for rid, row in m.items()} for t, m in state.items()}


def oracle_replay(db, target_batch_ord):
    """Full replay from seed, skipping the target, using original programs only."""
    rows = _rows_only(_initial_state(db))
    outcomes = {}
    for b in db.batches():
        bo = b["batch_ord"]
        if bo == target_batch_ord:
            outcomes[bo] = {"logical_batch_id": b["logical_batch_id"],
                            "status": "Superseded"}
            continue
        if b["status"] == "failed":
            # Originally failed: historical only, never activated.
            outcomes[bo] = {"logical_batch_id": b["logical_batch_id"],
                            "status": "Blocked", "reason": "historical_failure"}
            continue
        pre = _rows_only(rows)
        rows, info = ref.run_batch(b["program"], rows)
        if info["abort"] is not None:
            rows = pre  # atomic: a failing batch contributes no writes
            outcomes[bo] = {"logical_batch_id": b["logical_batch_id"],
                            "status": "Blocked",
                            "reason": info["abort"]["code"]}
        else:
            outcomes[bo] = {"logical_batch_id": b["logical_batch_id"],
                            "status": "Ran",
                            "reads": info["reads"], "writes": info["writes"]}
    return rows, outcomes


def compare_oracle(db, target_batch_ord, limit=50):
    expected_rows, expected_outcomes = oracle_replay(db, target_batch_ord)
    actual_rows = {}
    for t, rid, row in db.rows_for_generation(db.head()["generation"]):
        actual_rows.setdefault(t, {})[rid] = row
    for t in db.tables_for_generation(db.head()["generation"]):
        actual_rows.setdefault(t, {})
    diffs = []
    for t in sorted(set(expected_rows) | set(actual_rows)):
        exp = expected_rows.get(t, {})
        act = actual_rows.get(t, {})
        for rid in sorted(set(exp) | set(act)):
            e = exp.get(rid)
            a = act.get(rid)
            if e != a:
                diffs.append({"table": t, "row_id": rid, "expected": e,
                              "actual": a})
    return {"match": not diffs, "diffs": diffs,
            "expected_rows": expected_rows, "actual_rows": actual_rows,
            "expected_outcomes": expected_outcomes}


if __name__ == "__main__":
    from revoke_ledger.store import ManagedDB
    db = ManagedDB(sys.argv[1], create=False)
    target = db.batch_by_logical_id(sys.argv[3])
    cmp = compare_oracle(db, target["batch_ord"])
    print(json.dumps({"match": cmp["match"], "diffs": cmp["diffs"],
                      "outcomes": cmp["expected_outcomes"]}, indent=1))
    db.close()
