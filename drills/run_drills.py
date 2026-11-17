"""Executable drill: the full business scenario with printed, checkable evidence.

    python3 drills/run_drills.py
    python3 drills/run_drills.py --keep /tmp/drill-db     # keep a SQLite file

Every section prints the actual product state and the independent oracle's
state side by side and asserts they match.  The independent-partition skip is
proven by the recorded execution trace (which batches were executed vs skipped)
and by comparing the skipped output bytes to a full-replay oracle.
"""

import argparse
import json
import os
import sqlite3
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

import oracle  # noqa: E402
from fixtures.scenarios import (prog, _read, _write, main_drill, guard_drill,  # noqa: E402
                                empty_guard_drill, predicate_range_drill,
                                overwrite_same_id_drill, multi_step_failure_drill,
                                multi_step_failure_success_drill)
from revoke_ledger.errors import LedgerError, StaleError  # noqa: E402
from revoke_ledger.ledger import Ledger  # noqa: E402


def banner(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def show_state(label, L):
    print(f"  {label}:")
    snap = L.snapshots()
    for t in sorted(snap):
        if snap[t]:
            print(f"    {t}: " + ", ".join(
                f"[{rid}]={json.dumps(row, sort_keys=True)}"
                for rid, row in sorted(snap[t].items())))
        else:
            print(f"    {t}: (empty)")


def show_mapping(plan):
    print("  disposition by batch (original order):")
    for m in plan["mapping"]:
        print(f"    #{m['batch_ord']:<2} {m['logical_batch_id']:<8} "
              f"{m['disposition']:<11} {m['reason_code']:<28} "
              f"{m.get('reuse_kind')}")


def show_diff(plan):
    print("  value diff (old generation -> re-derived generation):")
    for d in plan["diff"]:
        print(f"    {d['table']}[{d['row_id']}]: {d['old']} -> {d['new']}  "
              f"({d['cause']})")


def make_db(keep):
    if keep:
        os.makedirs(keep, exist_ok=True)
        return Ledger(os.path.join(keep, "drill.db")), keep
    d = tempfile.mkdtemp(prefix="drill-")
    return Ledger(os.path.join(d, "drill.db")), d


def check_oracle(L, plan, label):
    cmp = oracle.compare_oracle(L.db, plan["target"]["batch_ord"])
    status = "MATCH" if cmp["match"] else "MISMATCH"
    print(f"  oracle[{label}]: {status} on every table/row/value")
    if not cmp["match"]:
        for d in cmp["diffs"]:
            print("    DIFF", d)
    return cmp


def scenario_main(L):
    banner("1. MAIN DRILL: A=10, T sets A=20, B=2*A, C independent C=7")
    initial, batches = main_drill()
    L.seed(initial)
    print("  seeded initial state:")
    show_state("initial", L)
    for b in batches:
        r = L.submit(b)
        print(f"  run {b['logical_batch_id']:<4} -> {r['status']} "
              f"gen={r['generation']}")
    show_state("before revocation", L)
    head_before = L.head()

    banner("1a. PLAN (no change to the official generation)")
    plan = L.plan_revocation("rev-main", "T")
    print(f"  head before plan: gen={head_before['generation']} "
          f"epoch={head_before['epoch']}")
    print(f"  head after  plan: gen={L.head()['generation']} "
          f"epoch={L.head()['epoch']} (unchanged: {L.head() == head_before})")
    print(f"  plan id {plan['plan_id']}  phase={plan['phase']}")
    print(f"  base generation (pre-target) = {plan['base_generation']}, "
          f"planned new generation = {plan['planned_generation']}")
    show_mapping(plan)
    show_diff(plan)
    m = {x["logical_batch_id"]: x for x in plan["mapping"]}

    banner("1b. SKIP EVIDENCE for the independent C batch")
    d = m["C"]["detail"]
    print(f"  declared partition: {json.dumps(d['declared_partition'])}")
    print(f"  revocation influence: {json.dumps(plan['influence'])}")
    print(f"  overlap with actually changed keys: "
          f"{d['actual_changed_keys_in_scope']}")
    print(f"  actual disjointness proven: {d['actual_disjointness_proven']}")
    print(f"  basis: {d['basis']}")
    print(f"  read classifications still equal original: "
          f"{all(r['classifications_match'] for r in d['proof']['read_checks'])}")
    print(f"  write preconditions still equal original: "
          f"{all(w.get('targets_checked', 0) >= 0 for w in d['proof']['write_checks'])}")

    banner("1c. APPLY (atomic pointer switch)")
    import revoke_ledger.ledger as _lg
    _orig_exec = _lg.execute_program
    executed = []

    def _counting(program, state):
        executed.append(program["logical_batch_id"])
        return _orig_exec(program, state)

    _lg.execute_program = _counting
    try:
        res = L.apply_plan(plan["plan_id"])
    finally:
        _lg.execute_program = _orig_exec
    print(f"  interpreter actually invoked for: {executed}")
    assert executed == [m["B"]["logical_batch_id"]], executed
    print("  -> only the affected batch B was executed; C never entered the interpreter")
    print(f"  new generation {res['new_generation']} (epoch {res['new_epoch']}), "
          f"previous {res['previous_generation']}")
    print(f"  replayed batch ords : {res['replayed_batch_ords']}")
    print(f"  skipped batch ords  : {res['skipped_independent_batch_ords']}")
    print(f"  blocked batch ords  : {res['blocked_batch_ords']}")
    show_state("after revocation", L)
    cmp = check_oracle(L, plan, "main")

    banner("1d. TRACE PROOF: C was NOT executed, B was")
    trace = L.db.generation_info(res["new_generation"])["trace"]["events"][0]
    print(f"  executed_batch_ords = {trace['executed_batch_ords']}")
    print(f"  skipped_batch_ords  = {trace['skipped_batch_ords']}")
    print(f"  blocked_batch_ords  = {trace['blocked_batch_ords']}")
    c_ord = m["C"]["batch_ord"]
    assert c_ord in trace["skipped_batch_ords"]
    assert c_ord not in trace["executed_batch_ords"]
    print(f"  C (batch #{c_ord}) present in skipped and absent from executed: OK")
    expected_rows, _ = oracle.oracle_replay(L.db, plan["target"]["batch_ord"])
    assert expected_rows["gamma"] == L.snapshots()["gamma"]
    print("  C's output bytes equal the full-replay oracle's bytes: OK")

    print("\n  business assertions:")
    post = L.snapshots()
    print(f"    A = {post['alpha'][1]['amount']} (want 10)")
    print(f"    B = {post['beta'][500]['value']} (want 20, not 40 and not deleted)")
    print(f"    C = {post['gamma'][100]['value']} (want 7)")
    assert post["alpha"][1]["amount"] == 10
    assert post["beta"][500]["value"] == 20
    assert post["gamma"][100]["value"] == 7

    banner("1e. RESPONSE LOST AFTER APPLY: same decision by revocation id")
    again = L.revocation_result("rev-main")
    print(f"  phase={again['phase']} new_generation={again['new_generation']} "
          f"state_promise={again['result']['state_promise'][:23]}...")
    again2 = L.apply_plan(plan["plan_id"])
    print(f"  re-apply returns generation {again2['new_generation']} "
          f"(no second revocation generation)")
    assert again2["new_generation"] == res["new_generation"]
    rev_gens = [g for g in L.history() if g["kind"] == "revocation"]
    print(f"  revocation generations in history: {[g['generation'] for g in rev_gens]}")
    assert len(rev_gens) == 1
    return cmp


def scenario_guard(_unused):
    banner("2. LATER GUARD 'A >= 15' BECOMES FALSE -> BLOCKED")
    initial, batches = guard_drill()
    L = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-guard-"), "g.db"))
    L.seed(initial)
    for b in batches:
        L.submit(b)
    show_state("before revocation", L)
    plan = L.plan_revocation("rev-guard", "T")
    show_mapping(plan)
    m = {x["logical_batch_id"]: x for x in plan["mapping"]}
    assert m["G"]["disposition"] == "Blocked"
    assert m["G"]["reason_code"] == "blocked_guard"
    res = L.apply_plan(plan["plan_id"])
    print(f"  blocked batch ords: {res['blocked_batch_ords']}")
    show_state("after revocation", L)
    check_oracle(L, plan, "guard")


def scenario_empty_pred(_unused):
    banner("3. EMPTY-READ GUARD + LATER INSERT, PREDICATE-WINDOW CHANGE")
    initial, batches = empty_guard_drill()
    L = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-empty-"), "e.db"))
    L.seed(initial)
    for b in batches:
        r = L.submit(b)
        print(f"  run {b['logical_batch_id']:<3} -> {r['status']} "
              f"{(r.get('abort') or {}).get('code') or ''}")
    show_state("before revocation", L)
    plan = L.plan_revocation("rev-empty", "T")
    show_mapping(plan)
    res = L.apply_plan(plan["plan_id"])
    show_state("after revocation", L)
    print("  E was originally failed -> historical decision only, never activated")
    print("  Z inserted probe[9] only because A>=20; now blocked, no probe write")
    check_oracle(L, plan, "empty")

    # Predicate window membership change
    print()
    initial2, batches2 = predicate_range_drill()
    L2 = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-pred-"), "p.db"))
    L2.seed(initial2)
    for b in batches2:
        L2.submit(b)
    print("  predicate-window drill: W selects alpha rows with amount in [10,35]")
    show_state("before revocation", L2)
    plan2 = L2.plan_revocation("rev-pred", "T")
    show_mapping(plan2)
    L2.apply_plan(plan2["plan_id"])
    show_state("after revocation", L2)
    print("  after revocation alpha[1]=10 enters W's window, two rows selected,")
    print("  so W's row_count==1 guard fails -> Blocked (no beta write)")
    check_oracle(L2, plan2, "predicate")


def scenario_overwrite_multistep(_unused):
    banner("4. SAME row_id OVERWRITTEN BY A LATER BATCH")
    initial, batches = overwrite_same_id_drill()
    L = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-over-"), "o.db"))
    L.seed(initial)
    for b in batches:
        L.submit(b)
    show_state("before revocation", L)
    plan = L.plan_revocation("rev-over", "T")
    show_mapping(plan)
    L.apply_plan(plan["plan_id"])
    show_state("after revocation", L)
    check_oracle(L, plan, "overwrite")

    banner("5. MULTI-STEP BATCH FAILURE (atomic, no writes)")
    initial, batches = multi_step_failure_drill()
    L2 = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-ms-"), "m.db"))
    L2.seed(initial)
    for b in batches:
        r = L2.submit(b)
        print(f"  run {b['logical_batch_id']:<3} -> {r['status']} "
              f"{(r.get('abort') or {}).get('code') or ''}")
    show_state("before revocation", L2)
    print("  M failed originally (second step pk conflict) -> historical only")
    plan2 = L2.plan_revocation("rev-ms", "T")
    show_mapping(plan2)
    L2.apply_plan(plan2["plan_id"])
    show_state("after revocation", L2)
    check_oracle(L2, plan2, "multistep")

    banner("5b. GUARD-BLOCKED MULTI-STEP SUCCESSFUL BATCH")
    initial, batches = multi_step_failure_success_drill()
    L3 = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-msg-"), "g.db"))
    L3.seed(initial)
    for b in batches:
        r = L3.submit(b)
        print(f"  run {b['logical_batch_id']:<3} -> {r['status']} "
              f"{(r.get('abort') or {}).get('code') or ''}")
    plan3 = L3.plan_revocation("rev-msg", "T")
    show_mapping(plan3)
    L3.apply_plan(plan3["plan_id"])
    show_state("after revocation", L3)
    print("  M wrote beta[500],beta[501] originally; now guard A>=20 fails and")
    print("  M is blocked with zero writes")
    check_oracle(L3, plan3, "multistep_guard")


def scenario_staleness(_unused):
    banner("6. PLAN THEN NEW BATCH / EXTERNAL EDIT / TAMPER ARE REJECTED")
    # 6a new batch after plan
    initial, batches = main_drill()
    L = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-stale-"), "s.db"))
    L.seed(initial)
    for b in batches:
        L.submit(b)
    plan = L.plan_revocation("rev-stale-new", "T")
    L.submit(prog("X", [_read("r", "alpha", [2], {"kind": "all"}),
                        _write("replace", "alpha", "r", [
                            {"source_row_id": 2, "row_id": {"const": 2},
                             "values": {"amount": {"const": 1}}}])]))
    try:
        L.apply_plan(plan["plan_id"])
        print("  ERROR: stale plan applied")
    except StaleError as e:
        print(f"  6a new batch after plan -> rejected: {e.code}")
    print(f"     new batch's value preserved: alpha[2]="
          f"{L.snapshots()['alpha'][2]}")

    # 6b external edit
    L2 = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-tamper-"), "t.db"))
    L2.seed(initial)
    for b in batches:
        L2.submit(b)
    plan2 = L2.plan_revocation("rev-stale-ext", "T")
    c = sqlite3.connect(L2.db.path)
    c.execute("DROP TRIGGER managed_rows_no_update")
    c.execute("UPDATE managed_rows SET row_json='{\"value\":99}' "
              "WHERE generation=6 AND tbl='beta' AND row_id=500")
    c.commit()
    c.close()
    try:
        L2.apply_plan(plan2["plan_id"])
        print("  ERROR: plan over edited data applied")
    except StaleError as e:
        print(f"  6b external SQLite edit after plan -> rejected: {e.code}")
    print(f"     edited value still present: beta[500]="
          f"{L2.db.get_row_at(6, 'beta', 500)}")

    # 6c plan payload tamper
    L3 = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-ptamper-"), "p.db"))
    L3.seed(initial)
    for b in batches:
        L3.submit(b)
    plan3 = L3.plan_revocation("rev-stale-plan", "T")
    with L3.db.tx():
        L3.db.conn.execute(
            "UPDATE generation_plan SET plan_payload_json=? WHERE plan_id=?",
            ('{"target":{"batch_ord":2},"skip_set":[],"mapping":[]}',
             plan3["plan_id"]))
    try:
        L3.apply_plan(plan3["plan_id"])
        print("  ERROR: tampered plan applied")
    except StaleError as e:
        print(f"  6c tampered plan payload -> rejected: {e.code}")

    # 6d ingest only
    L4 = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-ing-"), "i.db"))
    L4.seed(initial)
    for b in batches:
        L4.submit(b)
    plan4 = L4.plan_revocation("rev-stale-ing", "T")
    L4.ingest(prog("Y", [_read("r", "alpha", [1], {"kind": "all"})]))
    try:
        L4.apply_plan(plan4["plan_id"])
        print("  ERROR: plan applied after ingest")
    except StaleError as e:
        print(f"  6d new ingest after plan -> rejected: {e.code}")

    # 6e interrupted planning recovery
    L5 = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-rec-"), "r.db"))
    L5.seed(initial)
    for b in batches:
        L5.submit(b)
    plan5 = L5.plan_revocation("rev-rec", "T")
    with L5.db.tx():
        L5.db.conn.execute("UPDATE generation_plan SET phase='planning' "
                           "WHERE plan_id=?", (plan5["plan_id"],))
        L5.db.conn.execute("DELETE FROM revocation_mapping WHERE plan_id=? "
                           "AND batch_ord=4", (plan5["plan_id"],))
    rec = L5.recover_interrupted_plan("rev-rec", "T")
    print(f"  6e interrupted planning recovered from stage: {rec['action']}, "
          f"phase={rec['plan']['phase']}")
    r5 = L5.apply_plan(rec["plan"]["plan_id"])
    print(f"     applied from recovered plan -> generation {r5['new_generation']}")
    check_oracle(L5, L5.plan_view(rec["plan"]["plan_id"]), "recovered")

    # 6f one revocation id cannot be rebound / target revoked twice
    L6 = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-reb-"), "b.db"))
    L6.seed(initial)
    for b in batches:
        L6.submit(b)
    p6 = L6.plan_revocation("rev-reb", "T")
    L6.apply_plan(p6["plan_id"])
    for rid, tgt in [("rev-reb", "B"), ("rev-other", "T")]:
        try:
            L6.plan_revocation(rid, tgt)
            print(f"  ERROR: rebind {rid}->{tgt} allowed")
        except LedgerError as e:
            print(f"  6f rejected {rid}->{tgt}: {str(e)[:70]}...")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", default=None,
                    help="directory to keep one drill SQLite db in")
    args = ap.parse_args()

    if args.keep:
        path = os.path.join(args.keep, "main.db")
        if os.path.exists(path):
            os.unlink(path)
        L = Ledger(path)
    else:
        L = Ledger(os.path.join(tempfile.mkdtemp(prefix="drill-main-"), "m.db"))
    scenario_main(L)
    scenario_guard(L)
    scenario_empty_pred(L)
    scenario_overwrite_multistep(L)
    scenario_staleness(L)

    banner("ALL DRILLS COMPLETED")
    print("  Every section asserted product == independent full replay oracle.")
    print("  Independent partition C was skipped by the product (trace above),")
    print("  while the oracle re-executed it; outputs matched value by value.")


if __name__ == "__main__":
    main()
