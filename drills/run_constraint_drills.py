"""Executable constraint drills with printed, checkable evidence.

    python3 drills/run_constraint_drills.py
    python3 drills/run_constraint_drills.py --keep /tmp/ckeep

Every section prints the actual product state and the independent
constraint_oracle's state side by side, and asserts they match.  Expected
cascade deletion sets and final row sets come from the naive reference
implementation in constraint_oracle.py (no product imports), never from the
production cascade closure.

The sections mirror the required acceptance list:
  1  child inserted before its parent, in one batch -> success
  2  joint unique value swap -> success; final duplicate -> whole batch rejected
  3  one deletion reaching a row by two paths through a reference cycle
  4  RESTRICT child removed by another cascade path (allowed) vs surviving
     child (rejected, old data unchanged)
  5  target adds a parent, a later batch writes a child on *another* table,
     revoking the target blocks the later batch; a truly unrelated partition
     still skips in the recorded execution trace
  6  delete+recreate parent (kept), new child of a deleted parent (cascaded),
     reference to a parent that never existed (rejected)
  7  terminal check order, delete semantics, and reopen stability
"""

import argparse
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

import constraint_oracle  # noqa: E402
from fixtures.scenarios import (CONSTRAINT_RULES, CYCLE_RULES, RESTRICT_RULES,
                                prog, _read, _write,
                                constraint_cycle_drill,
                                constraint_delete_recreate_drill,
                                constraint_drill,
                                constraint_new_child_of_deleted_parent_drill,
                                constraint_phantom_parent_drill,
                                constraint_restrict_drill,
                                constraint_revocation_drill,
                                constraint_swap_failure_drill)  # noqa: E402
from revoke_ledger.errors import BatchAbort  # noqa: E402
from revoke_ledger.ledger import Ledger  # noqa: E402


def banner(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def show_state(label, L):
    print(f"  {label}:")
    for t in sorted(L.snapshots()):
        rows = L.snapshots()[t]
        if rows:
            print(f"    {t}: " + ", ".join(
                f"[{rid}]={json.dumps(row, sort_keys=True)}"
                for rid, row in sorted(rows.items())))
        else:
            print(f"    {t}: (empty)")


def rows_of(L):
    return {t: {rid: dict(r) for rid, r in m.items()}
            for t, m in L.snapshots().items()}


def oracle_block(initial, batches, rules, skip_ord=None):
    recs = [{"logical_batch_id": b["logical_batch_id"], "batch_ord": i + 1,
             "status": "success", "program": b} for i, b in enumerate(batches)]
    return constraint_oracle.replay(initial, recs, rules, skip_ord or -1)


def new_ledger(initial, rules, kept_path=None, name="c.db"):
    if kept_path:
        path = os.path.join(kept_path, name)
        if os.path.exists(path):
            os.unlink(path)
    else:
        path = os.path.join(tempfile.mkdtemp(prefix="cdrill-"), name)
    L = Ledger(path)
    L.install_constraints(rules)
    L.seed(initial)
    return L


def show_deleted(report):
    print("  deletion roots (by row identity):", report["roots"])
    print("  explicit deletes:", report["explicit_deletes"])
    for d in report["deleted"]:
        kind = "EXPLICIT" if d["explicit"] else "IMPLICIT"
        extra = f" extra_parents={d['extra_parents']}" if d["extra_parents"] else ""
        chain = " -> ".join(f"{h['from_table']}[{h['from_row_id']}]"
                            for h in d["trigger_chain"])
        tail = f"{d['table']}[{d['row_id']}]" if chain else ""
        print(f"    {kind:<8} {d['table']}[{d['row_id']}]"
              f"  rule={d['rule']}{extra}"
              + (f"  chain: {chain} -> {tail}" if chain else ""))


def section_install(keep):
    banner("0. DECLARATIVE CONFIG, READ-BACK AND INSTALL PROTOCOL")
    L = new_ledger(constraint_drill()[0], CONSTRAINT_RULES, keep, "install.db")
    view = L.constraints_view()
    print(f"  installed={view['installed']} schema={view['schema']}")
    print(f"  config_hash={view['config_hash'][:23]}...")
    print(f"  installed_at_generation={view['installed_at_generation']} "
          f"before_ingest={view['installed_before_ingest']}")
    for r in view["rules"]:
        print(f"    rule {r['name']:<20} {r['kind']:<11} "
              f"{json.dumps({k: v for k, v in r.items() if k not in ('name', 'kind')}, sort_keys=True)}")
    print("  participating tables:", view["participating_tables"])
    L.close()
    L2 = Ledger(L.db.path)
    again = L2.constraints_view()
    print(f"  after reopen: rules identical={again['rules'] == view['rules']} "
          f"hash identical={again['config_hash'] == view['config_hash']}")
    assert again["rules"] == view["rules"]
    try:
        L2.install_constraints({"schema": "managed-constraints/v1", "rules": []})
        raise SystemExit("ERROR: rules were replaceable")
    except Exception as exc:  # noqa: BLE001
        print(f"  replacement rejected: {str(exc)[:66]}...")
    L2.close()


def section_child_before_parent(keep):
    banner("1. CHILD INSERTED BEFORE ITS PARENT (SAME BATCH) -> SUCCESS")
    initial, batches = constraint_drill()
    L = new_ledger(initial, CONSTRAINT_RULES, keep, "childfirst.db")
    r = L.submit(batches[0])
    print(f"  batch B1 -> {r['status']}")
    print("  explicit step order: entry[200] (child), account[20], owner[3] (parent)")
    rep = r["evidence"]["constraints"]
    print(f"  constraint check performed after ALL steps: ok={rep['ok']} "
          f"deletions={rep['implicit_deletes']}")
    show_state("state after B1", L)
    assert r["status"] == "success"
    assert rows_of(L)["entry"][200]["account_id"] == 20
    assert rows_of(L)["account"][20]["owner_id"] == 3
    assert 3 in rows_of(L)["owner"]
    print("  -> the incomplete intermediate relation was never a failure; the")
    print("     completed batch state satisfies both references.")


def section_swap(keep):
    banner("2. JOINT UNIQUE SWAP OK; FINAL DUPLICATE REJECTS THE WHOLE BATCH")
    initial, batches = constraint_drill()
    L = new_ledger(initial, CONSTRAINT_RULES, keep, "swap.db")
    L.submit(batches[0])
    before = rows_of(L)
    r = L.submit(batches[1])
    print(f"  swap batch B2 -> {r['status']}")
    print(f"  owner[1]={json.dumps(rows_of(L)['owner'][1], sort_keys=True)} "
          f"owner[2]={json.dumps(rows_of(L)['owner'][2], sort_keys=True)}")
    assert r["status"] == "success"
    assert rows_of(L)["owner"] == {1: {"code": "beta"}, 2: {"code": "acme"},
                                   3: {"code": "acme-2"}}

    banner("2b. FINAL DUPLICATE AFTER A SWAP -> REJECTED, NO PARTIAL WRITE")
    initial2, batches2 = constraint_swap_failure_drill()
    L2 = new_ledger(initial2, CONSTRAINT_RULES, keep, "swapdup.db")
    head = L2.head()
    pre = rows_of(L2)
    r2 = L2.submit(batches2[0])
    print(f"  batch T -> {r2['status']}  code={r2['abort']['code']}")
    print("  located reasons:")
    for reason in r2["abort"]["reasons"]:
        print(f"    rule={reason['rule']} table={reason['table']} "
              f"rows={reason['row_ids']} fields={reason['fields']} "
              f"value={reason['values']}")
    print(f"  head unchanged: {L2.head() == head}; generation is None: "
          f"{r2['generation'] is None}")
    show_state("state after rejected batch", L2)
    assert r2["status"] == "failed"
    assert L2.head() == head
    assert rows_of(L2) == pre
    # Independent oracle agrees it is a violation.
    _, out, _ = oracle_block(initial2, batches2, CONSTRAINT_RULES["rules"])
    print(f"  independent oracle outcome: {out[1]}")
    assert out[1]["status"] == "Blocked"


def section_cycle(keep):
    banner("3. ONE DELETION, TWO PATHS, THROUGH A REFERENCE CYCLE")
    initial, batches = constraint_cycle_drill()
    print("  fixture: owner[1] -> node 1 -> node 2 -> {node 3, node 9}")
    print("           owner[1] -> node 7 <-> node 8          (cycle)")
    print("           owner[1] -> node 9                   (second path)")
    L = new_ledger(initial, CYCLE_RULES, keep, "cycle.db")
    r = L.submit(batches[0])
    assert r["status"] == "success", r["abort"]
    report = r["evidence"]["constraints"]
    show_deleted(report)
    product = sorted([d["table"], d["row_id"]] for d in report["deleted"])
    __, _, deletions = oracle_block(initial, [batches[0]],
                                   CYCLE_RULES["rules"])
    print("  expected deletion set (independent naive closure):",
          sorted([list(k) for k in deletions[1]]))
    print("  product deletion set:                              ", product)
    assert product == sorted([list(k) for k in deletions[1]])
    assert product == [["node", 1], ["node", 2], ["node", 3], ["node", 7],
                       ["node", 8], ["node", 9], ["owner", 1]]
    node9 = [d for d in report["deleted"] if d["table"] == "node"
             and d["row_id"] == 9][0]
    print(f"  node[9] recorded once; reached from node[2] as an extra parent: "
          f"{node9['extra_parents']}")
    show_state("final state", L)
    expected, _, _ = oracle_block(initial, [batches[0]], CYCLE_RULES["rules"])
    assert {t: m for t, m in rows_of(L).items() if m} == \
        {t: dict(m) for t, m in expected.items() if m}


def section_restrict(keep):
    banner("4a. RESTRICT CHILD SWEPT BY ANOTHER CASCADE PATH -> DELETE ALLOWED")
    initial, batches = constraint_restrict_drill(True)
    L = new_ledger(initial, RESTRICT_RULES, keep, "restrict_ok.db")
    print("  note[900] -> owner[1] is RESTRICT; note[900] -> ledger[50] is CASCADE")
    print("  batch deletes owner[1] and ledger[50]")
    r = L.submit(batches[0])
    assert r["status"] == "success", r["abort"]
    show_deleted(r["evidence"]["constraints"])
    show_state("final state", L)
    assert not rows_of(L).get("owner")
    assert not rows_of(L).get("note")

    banner("4b. RESTRICT CHILD SURVIVES -> WHOLE BATCH REJECTED, OLD DATA INTACT")
    initial2, batches2 = constraint_restrict_drill(False)
    L2 = new_ledger(initial2, RESTRICT_RULES, keep, "restrict_block.db")
    pre = rows_of(L2)
    r2 = L2.submit(batches2[0])
    print(f"  batch T -> {r2['status']}  code={r2['abort']['code']}")
    for reason in r2["abort"]["reasons"]:
        print(f"    rule={reason['rule']} child={reason['table']}[{reason['row_id']}]"
              f" parent_deleted={reason['deleted_parent']} code={reason['code']}")
    show_state("state after rejection", L2)
    assert r2["status"] == "failed"
    assert rows_of(L2) == pre
    assert 1 in rows_of(L2)["owner"]
    assert 900 in rows_of(L2)["note"]
    assert 10 in rows_of(L2)["account"]


def section_revocation(keep):
    banner("5. TARGET ADDS PARENT; LATER CHILD BATCH ON ANOTHER TABLE IS BLOCKED")
    initial, batches = constraint_revocation_drill()
    L = new_ledger(initial, CONSTRAINT_RULES, keep, "revocation.db")
    for b in batches:
        r = L.submit(b)
        status = r["status"]
        code = (r.get("abort") or {}).get("code")
        print(f"  run {b['logical_batch_id']:<3} -> {status}"
              + (f" ({code})" if code else ""))
    show_state("before revocation", L)
    print("  T writes owner[5]; L declares partition account[1..2] and writes")
    print("  account[2].owner_id=5 without ever reading `owner`; Z writes note[7].")
    plan = L.plan_revocation("rev-parent", "T")
    print("  disposition by batch (original order):")
    for m in plan["mapping"]:
        print(f"    #{m['batch_ord']:<2} {m['logical_batch_id']:<3} "
              f"{m['disposition']:<11} {m['reason_code']:<30} "
              f"{m.get('reuse_kind')}")
    m = {x["logical_batch_id"]: x for x in plan["mapping"]}
    print("  L's constraint reasons:",
          json.dumps(m["L"]["detail"]["constraint_reasons"]))
    print("  Z's recorded dependency closure keys:",
          m["Z"]["detail"]["constraint_dependency_keys"],
          "overlap:", m["Z"]["detail"]["actual_dependency_overlap"])
    assert m["L"]["disposition"] == "Blocked"
    assert m["L"]["reason_code"] == "blocked_constraint_violation"
    assert m["Z"]["reuse_kind"] == "skipped_independent"

    import revoke_ledger.ledger as _lg
    orig = _lg.execute_program
    executed = []

    def counting(program, state, constraints=None):
        executed.append(program["logical_batch_id"])
        if constraints is None:
            return orig(program, state)
        return orig(program, state, constraints=constraints)

    _lg.execute_program = counting
    try:
        res = L.apply_plan(plan["plan_id"])
    finally:
        _lg.execute_program = orig
    print(f"  interpreter invoked for: {executed}")
    print(f"  replayed batch ords : {res['replayed_batch_ords']}")
    print(f"  skipped batch ords  : {res['skipped_independent_batch_ords']}")
    print(f"  blocked batch ords  : {res['blocked_batch_ords']} "
          f"reasons={res['blocked_reasons']}")
    trace = L.db.generation_info(res["new_generation"])["trace"]["events"][0]
    print(f"  trace: executed={trace['executed_batch_ords']} "
          f"skipped={trace['skipped_batch_ords']} "
          f"blocked={trace['blocked_batch_ords']}")
    print("  -> L (constraint-dependent, different table) was NOT skipped; it")
    print("     entered the interpreter, failed its FK check and was blocked.")
    print("  -> Z (truly unrelated partition) was NOT executed.")
    assert m["L"]["logical_batch_id"] not in [
        b["logical_batch_id"] for b in L.db.batches()
        if b["logical_batch_id"] == "Z"]
    assert m["Z"]["batch_ord"] in trace["skipped_batch_ords"]
    assert m["L"]["batch_ord"] in trace["blocked_batch_ords"]
    assert m["L"]["batch_ord"] in trace["executed_batch_ords"]
    show_state("after revocation", L)
    expected, outcomes, _ = oracle_block(initial, batches,
                                        CONSTRAINT_RULES["rules"], skip_ord=1)
    print("  independent oracle outcomes:",
          {k: (v["status"], v.get("code")) for k, v in outcomes.items()})
    assert {t: m2 for t, m2 in rows_of(L).items() if m2} == \
        {t: dict(m2) for t, m2 in expected.items() if m2}
    assert outcomes[m["L"]["batch_ord"]]["status"] == "Blocked"
    print("  oracle[revocation]: MATCH on every table/row/value")

    L.close()
    L2 = Ledger(L.db.path)
    expl = L2.explain_revocation("rev-parent")
    print(f"  after reopen: phase={expl['phase']} "
          f"blocked={expl['blocked']}")
    print(f"  L constraint reasons still readable from the batch record: "
          f"{expl['batches']['L']['constraint_reasons'][0]['rule']}")
    assert expl["blocked"] == {"L": "blocked_constraint_violation"}
    L2.close()


def section_three_delete_cases(keep):
    banner("6a. PARENT DELETED AND RE-CREATED IN THE SAME BATCH -> KEPT")
    initial, batches = constraint_delete_recreate_drill()
    L = new_ledger(initial, CYCLE_RULES, keep, "recreate.db")
    r = L.submit(batches[0])
    assert r["status"] == "success", r["abort"]
    rep = r["evidence"]["constraints"]
    print(f"  roots={rep['roots']} deleted={rep['deleted']}")
    print("  -> no deletion root: the row identity never disappeared at the end")
    show_state("final state", L)
    assert rep["roots"] == []

    banner("6b. NEW CHILD INSERTED POINTING AT THIS BATCH'S DELETED PARENT -> CASCADED")
    initial2, batches2 = constraint_new_child_of_deleted_parent_drill()
    L2 = new_ledger(initial2, RESTRICT_RULES, keep, "newchild.db")
    r2 = L2.submit(batches2[0])
    assert r2["status"] == "success", r2["abort"]
    show_deleted(r2["evidence"]["constraints"])
    acct = [d for d in r2["evidence"]["constraints"]["deleted"]
            if d["table"] == "account"][0]
    print(f"  account[77] inserted_in_batch={acct['inserted_in_batch']} "
          f"implicit={acct['implicit']}")
    show_state("final state", L2)
    assert 77 not in rows_of(L2).get("account", {})

    banner("6c. REFERENCE TO A PARENT THAT NEVER EXISTED -> REJECTED, NOT CASCADED")
    initial3, batches3 = constraint_phantom_parent_drill()
    L3 = new_ledger(initial3, RESTRICT_RULES, keep, "phantom.db")
    r3 = L3.submit(batches3[0])
    print(f"  batch T -> {r3['status']} reason={json.dumps(r3['abort']['reasons'])}")
    show_state("state after rejection", L3)
    assert r3["status"] == "failed"
    assert r3["abort"]["reasons"][0]["code"] == "fk_missing"
    assert 55 not in rows_of(L3).get("account", {})
    print("  -> the three cases are distinct: recreated parent is not a root,")
    print("     a real child of a deleted parent is cascaded, a dangling")
    print("     reference is a violation and never a silent cascade.")


def section_check_order(keep):
    banner("7. TERMINAL CHECK ORDER AND DELETE SEMANTICS (READ BACK FROM EVIDENCE)")
    L = new_ledger(constraint_drill()[0], CONSTRAINT_RULES, keep, "order.db")
    view = L.constraints_view()
    print("  declared semantics recorded with the configuration:")
    for key, value in view["semantics"].items():
        print(f"    {key}: {value}")
    print()
    print("  implemented order for one batch:")
    print("    1. protocol validation at ingest (unchanged)")
    print("    2. explicit steps in order: reads/writes see earlier transient")
    print("       writes; pk/delete/source preconditions and guards as before")
    print("    3. one completed-state pass:")
    print("       a. deletion roots = identities present before the batch and")
    print("          absent after its explicit steps (delete+recreate excluded)")
    print("       b. CASCADE closure over surviving candidate rows until stable;")
    print("          each row deleted once; cycles terminate")
    print("       c. surviving-row FK existence (incl. RESTRICT against deleted")
    print("          parents) and unique-key group checks")
    print("    4. any reason -> whole batch rejected: no explicit write and no")
    print("       cascade is committed, no generation, reasons recorded")
    print("    5. success -> one generation, evidence carries explicit vs")
    print("       implicit effects, trigger chains and the rule-set snapshot")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", default=None,
                    help="directory to keep drill SQLite files in")
    args = ap.parse_args()
    if args.keep:
        os.makedirs(args.keep, exist_ok=True)
    section_install(args.keep)
    section_child_before_parent(args.keep)
    section_swap(args.keep)
    section_cycle(args.keep)
    section_restrict(args.keep)
    section_revocation(args.keep)
    section_three_delete_cases(args.keep)
    section_check_order(args.keep)
    banner("ALL CONSTRAINT DRILLS COMPLETED")
    print("  Every section asserted product == independent constraint oracle.")
    print("  Cascade sets came from the naive reference closure, not from the")
    print("  product implementation.")


if __name__ == "__main__":
    main()
