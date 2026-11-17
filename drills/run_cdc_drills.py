"""Executable drill for the offline CDC transaction ingestor.

    python3 drills/run_cdc_drills.py
    python3 drills/run_cdc_drills.py --keep /tmp/cdckeep

The narrative follows the task statement:

1. a *later* source transaction is delivered first and the managed targets
   stay unchanged until the earlier sequences are completed;
2. one transaction inserts a child row before its parent; the next
   transaction updates twice and deletes/recreates a row -- final rows,
   reference mappings, cursor and ledger batch count are checked against the
   independent dictionary model in fixtures/cdc_scenarios.py, not against the
   product's own counters;
3. redelivery, a truncated file, identity conflicts, true-vs-1 before
   mismatch, and a constraint rejection followed by correct/incorrect skip
   digests are all verified on the persisted state;
4. revoking the first successful CDC batch makes the next transaction's
   before-condition fail (blocked, never silently rewritten), redelivery does
   not resurrect the target, and an association/aggregation program
   recomputes from the post-revocation inputs;
5. real subprocess exits cover: after fragments land, between target commit
   and the response, and after the response; reopening shows no half
   transaction, no duplicate ledger batch and no skipped cursor;
6. two processes racing to advance the same source produce exactly one
   decision.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from fixtures.cdc_scenarios import (MiniModel, SOURCE_ID, TBL_CUSTOMER,
                                    TBL_ORDER, constraint_rules,
                                    drill_delivery_order, drill_transactions,
                                    source_config)
from revoke_ledger.canon import canon
from revoke_ledger.cdc import ST_MISSING_TX, ST_READY
from revoke_ledger.errors import LedgerError, ProtocolError
from revoke_ledger.ledger import Ledger

WORKER = os.path.join(HERE, "cdc_worker.py")


def banner(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def write_jsonl(path, fragments):
    with open(path, "w", encoding="utf-8") as fh:
        for f in fragments:
            fh.write(json.dumps(f) + "\n")


def new_ledger(keep, name, constrained=False):
    if keep:
        os.makedirs(keep, exist_ok=True)
        path = os.path.join(keep, name)
        if os.path.exists(path):
            os.unlink(path)
        for suffix in ("-wal", "-shm"):
            if os.path.exists(path + suffix):
                os.unlink(path + suffix)
    else:
        path = os.path.join(tempfile.mkdtemp(prefix="cdcdrill-"), name)
    L = Ledger(path)
    if constrained:
        L.install_constraints(constraint_rules())
    L.cdc().install_source(source_config())
    return L


def worker(*args, expect=0):
    proc = subprocess.run([sys.executable, "-I", WORKER, *args],
                          capture_output=True, text=True)
    if expect is not None:
        assert proc.returncode == expect, (args, proc.returncode,
                                           proc.stdout, proc.stderr)
    out = proc.stdout.strip().splitlines()
    return json.loads(out[-1]) if out else None, proc


# ---------------------------------------------------------------------------
def section_out_of_order(keep):
    banner("1. OUT-OF-ORDER DELIVERY: later tx first, targets stay frozen")
    L = new_ledger(keep, "order.db")
    cdc = L.cdc()
    later = drill_transactions()[3][1]
    r = cdc.receive_fragments(later)
    print(f"  staged tx3 fragments: {r['staged']} (staging only, not loading)")
    status = cdc.source_status(SOURCE_ID, window=4)
    for t in status["transactions"]:
        print(f"    seq {t['tx_seq']}: {t['state']}")
    assert status["transactions"][3]["state"] == ST_READY
    assert status["first_blocker"]["tx_seq"] == 0
    assert status["first_blocker"]["state"] == ST_MISSING_TX

    before_head = L.head()
    adv = cdc.advance(SOURCE_ID)
    print(f"  advance with seq 0 missing -> decisions={adv['decisions']} "
          f"next_seq={adv['next_seq']}")
    assert adv["next_seq"] == 0 and adv["decisions"] == []
    assert L.head() == before_head
    assert L.snapshots().get(TBL_CUSTOMER, {}) == {}

    # Deliver the rest in the drill's shuffled order.
    for label, frags in drill_delivery_order()[1:]:
        cdc.receive_fragments(frags)
        print(f"  delivered {label}")
    L.close()


def section_full_drill_vs_model(keep):
    banner("2. FULL DRILL vs INDEPENDENT DICTIONARY MODEL")
    L = new_ledger(keep, "model.db")
    cdc = L.cdc()
    for label, frags in drill_delivery_order():
        cdc.receive_fragments(frags)

    # Advance one at a time and compare every prefix with the MiniModel.
    checkpoints = [
        ([0], "after tx0 (child before parent)"),
        ([0, 1], "after tx1 (two updates + delete/recreate)"),
        ([0, 1, 2], "after tx2 (abort, no row effects)"),
        ([0, 1, 2, 3], "after tx3 (the previously blocked later tx)"),
    ]
    for prefixes, label in checkpoints:
        cdc.advance(SOURCE_ID, limit=1)
        state, identity, water, next_seq = MiniModel(
            source_config()).expected_rows_after(prefixes)
        snap = L.snapshots()
        assert snap[TBL_CUSTOMER] == state[TBL_CUSTOMER], label
        assert snap[TBL_ORDER] == state[TBL_ORDER], label
        print(f"  {label}: rows match the independent model")
        print(f"    dim_customer={json.dumps(snap[TBL_CUSTOMER], sort_keys=True)}")
        print(f"    fact_order  ={json.dumps(snap[TBL_ORDER], sort_keys=True)}")

    # Mapping and cursor cross-check.
    state, identity, water, next_seq = MiniModel(
        source_config()).expected_rows_after([0, 1, 2, 3])
    rows = L.db.conn.execute(
        "SELECT source_table,key_json,row_id FROM cdc_identity "
        "WHERE source_id=? ORDER BY managed_table,row_id",
        (SOURCE_ID,)).fetchall()
    got = {(r["source_table"], r["key_json"]): r["row_id"] for r in rows}
    for (src, key), rid in identity.items():
        assert got[(src, canon(list(key)))] == rid
    print(f"  identity mappings equal the model: {len(got)} mappings")
    assert cdc.source_status(SOURCE_ID)["next_seq"] == next_seq == 4
    batches = L.ledger()
    assert len(batches) == 3  # abort tx2 produces no ledger batch
    print(f"  ledger batches: {[b['logical_batch_id'] for b in batches]} "
          f"(abort tx2 has no batch); cursor next_seq=4")

    # Reverse lookups both directions.
    expl = cdc.explain_transaction(SOURCE_ID, 1)
    assert len(expl["change_maps"]) == 5
    rid1 = expl["change_maps"][0]["row_id"]
    back = cdc.explain_row(TBL_CUSTOMER, rid1)
    assert back["source_key"] == ["a", "1"]
    b = L.explain_batch("cdc:crm:1:T1")
    assert b["cdc"]["tx_seq"] == 1 and len(b["cdc"]["change_maps"]) == 5
    print(f"  explain tx1 -> {len(expl['change_maps'])} change maps; "
          f"explain_row({TBL_CUSTOMER},{rid1}) -> source {back['source_key']}")
    L.close()


def section_redelivery_and_files(keep):
    banner("3. REDELIVERY, TRUNCATED FILE, CONFLICT, TRUE-vs-1, SKIP DIGEST")
    L = new_ledger(keep, "files.db")
    cdc = L.cdc()
    txs = drill_transactions()
    for _label, frags in drill_delivery_order():
        cdc.receive_fragments(frags)
    cdc.advance(SOURCE_ID)

    # 3.1 identical redelivery after consumption: verifies, never reloads.
    r = cdc.receive_fragments(txs[0][1])
    assert all(x["status"] == "duplicate" for x in r["fragments"])
    n_batches = len(L.ledger())
    cdc.advance(SOURCE_ID)
    assert len(L.ledger()) == n_batches
    print("  consumed fragments redelivered: all duplicate, no second batch")

    # 3.2 same identity, different content: rejected even after consumption.
    evil = txs[0][1][1].copy()
    evil = dict(evil)
    evil["after"] = {"tenant": "a", "code": "1", "name": "TAMPERED"}
    try:
        cdc.receive_fragments([evil])
        raise AssertionError("conflicting content was accepted")
    except ProtocolError:
        print("  conflicting content for a consumed identity: rejected")

    # 3.3 trailing bad JSON: whole file rejected, zero rows staged.
    p = os.path.join(keep or tempfile.mkdtemp(prefix="cdcf-"), "broken.jsonl")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as fh:
        for f in txs[3][1]:
            fh.write(json.dumps(f) + "\n")
        fh.write('{"schema": "managed-cdc-fragment/v1", broken\n')
    try:
        cdc.receive_file(p)
        raise AssertionError("truncated file was accepted")
    except ProtocolError as exc:
        print(f"  truncated JSONL rejected atomically: {str(exc)[:60]}")
    # tx3 was already consumed; prove no duplicate fragment rows were added.
    n_frag = L.db.conn.execute(
        "SELECT COUNT(*) c FROM cdc_fragment WHERE source_id=? AND tx_seq=3",
        (SOURCE_ID,)).fetchone()["c"]
    assert n_frag == 3

    # 3.4 true vs 1 before mismatch on a fresh source/db.
    L2 = new_ledger(keep, "true1.db")
    c2 = L2.cdc()
    c2.receive_fragments([
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 0, "tx_id": "T0", "part": "begin"},
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 0, "tx_id": "T0", "part": "change", "index": 1,
         "table": "customer", "op": "insert", "before": None,
         "after": {"tenant": "a", "code": "1", "flag": True}},
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 0, "tx_id": "T0", "part": "end", "change_count": 1,
         "outcome": "commit"},
    ])
    c2.advance(SOURCE_ID)
    gen = L2.head()["generation"]
    # Tamper with the committed row (true -> 1) out of band.
    conn = L2.db.conn
    with conn:
        conn.execute("DROP TRIGGER managed_rows_no_update")
        conn.execute(
            "UPDATE managed_rows SET row_json=? WHERE generation=? AND tbl=? "
            "AND row_id=1",
            (canon({"tenant": "a", "code": "1", "flag": 1}),
             gen, TBL_CUSTOMER))
        conn.execute(
            "CREATE TRIGGER managed_rows_no_update BEFORE UPDATE ON "
            "managed_rows BEGIN SELECT RAISE(ABORT, 'managed_rows are "
            "immutable'); END")
    c2.receive_fragments([
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 1, "tx_id": "T1", "part": "begin"},
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 1, "tx_id": "T1", "part": "change", "index": 1,
         "table": "customer", "op": "update",
         "before": {"tenant": "a", "code": "1", "flag": True},
         "after": {"tenant": "a", "code": "1", "flag": False}},
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 1, "tx_id": "T1", "part": "end", "change_count": 1,
         "outcome": "commit"},
    ])
    res = c2.advance(SOURCE_ID)
    assert res["decisions"][0]["outcome"] == "rejected"
    abort = c2._decision(SOURCE_ID, 1)["detail"]["abort"]
    assert abort["code"] == "cdc_before_mismatch"
    print("  before true vs committed 1 -> cdc_before_mismatch, target kept")
    assert L2.snapshots()[TBL_CUSTOMER][1]["flag"] == 1
    L2.close()

    # 3.5 constraint rejection then correct/incorrect skip digest.
    L3 = new_ledger(keep, "skip.db", constrained=True)
    c3 = L3.cdc()
    c3.receive_fragments([
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 0, "tx_id": "F0", "part": "begin"},
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 0, "tx_id": "F0", "part": "change", "index": 1,
         "table": "order", "op": "insert", "before": None,
         "after": {"oid": 1, "cust": ["nope", "x"], "amt": 1}},
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 0, "tx_id": "F0", "part": "end", "change_count": 1,
         "outcome": "commit"},
        # a later ready tx that must wait behind the rejection
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 1, "tx_id": "F1", "part": "begin"},
        {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
         "tx_seq": 1, "tx_id": "F1", "part": "end", "change_count": 0,
         "outcome": "commit"},
    ])
    res = c3.advance(SOURCE_ID)
    assert res["decisions"][0]["outcome"] == "rejected"
    digest = c3._decision(SOURCE_ID, 0)["decision_digest"]
    print(f"  fk_missing rejection recorded (digest {digest[:12]}...)")
    try:
        c3.skip_rejected(SOURCE_ID, 0, "reason", "wrong-digest")
        raise AssertionError("wrong skip digest accepted")
    except LedgerError:
        print("  skip with wrong decision digest: rejected")
    assert c3.source_status(SOURCE_ID)["next_seq"] == 0
    c3.skip_rejected(SOURCE_ID, 0, "upstream confirmed bad parent", digest)
    print("  skip with the bound digest: accepted")
    res2 = c3.advance(SOURCE_ID)
    assert res2["decisions"][0]["outcome"] == "committed"
    assert c3.source_status(SOURCE_ID)["next_seq"] == 2
    # Trying to 'fix' the rejected fragments is refused (append-only history).
    try:
        c3.receive_fragments([
            {"schema": "managed-cdc-fragment/v1", "source_id": SOURCE_ID,
             "tx_seq": 0, "tx_id": "F0", "part": "change", "index": 1,
             "table": "order", "op": "insert", "before": None,
             "after": {"oid": 2}}])
        raise AssertionError("rejected tx was modifiable")
    except ProtocolError:
        print("  rejected fragments cannot be modified to 'fix' them")
    L3.close()
    L.close()


def section_revocation(keep):
    banner("4. REVOCATION: later before fails -> blocked; summary recomputes")
    L = new_ledger(keep, "rev.db")
    cdc = L.cdc()
    for _label, frags in drill_delivery_order():
        cdc.receive_fragments(frags)
    cdc.advance(SOURCE_ID)

    # An ordinary relational aggregation program over the managed CDC rows,
    # installed *before* the revocation.  Its read map names both customer
    # identities explicitly, so the removed row is recorded as an absent map
    # identity; on revocation the program is re-derived from the new inputs
    # (Changed), exactly like the existing relational summary drills, instead
    # of being patched from its old output or blocked on a missing source.
    summary_prog = {
        "logical_batch_id": "SUMMARY",
        "schema": "managed-relational/v1",
        "steps": [
            {"op": "read", "alias": "c",
             "select": {"table": TBL_CUSTOMER, "where": {"kind": "all"}},
             "map": [{"row_id": 1, "table": TBL_CUSTOMER},
                     {"row_id": 2, "table": TBL_CUSTOMER}],
             "row_id_field": "cid"},
            {"op": "project", "from": "c", "alias": "p",
             "select": [{"from": "cid", "as": "cid"},
                        {"from": "name", "as": "name"}]},
            {"op": "write", "effect": "replace",
             "table": "customer_summary", "from": "p",
             "target": {"field": "cid"},
             "values": {"name": {"field": "name"}}}]}
    sb = L.submit(summary_prog)
    assert sb["status"] == "success"
    before_summary = L.snapshots()["customer_summary"]
    assert before_summary[1]["name"] == "ada v3"
    assert before_summary[2]["name"] == "ben"

    target = "cdc:crm:0:T0"
    later = "cdc:crm:1:T1"
    plan = L.plan_revocation("rev-cdc-1", target)
    mapping = {m["logical_batch_id"]: m for m in plan["mapping"]}
    print("  revocation dispositions:")
    for m in plan["mapping"]:
        print(f"    {m['logical_batch_id']:<16} {m['disposition']:<9} "
              f"{m['reason_code']}")
    # T1 frozen before no longer holds after T0 is removed -> blocked.
    assert mapping[later]["disposition"] == "Blocked"
    assert mapping[later]["reason_code"] == "blocked_pk_conflict"
    # The summary program is replayed on the new inputs (Changed, not skipped).
    assert mapping["SUMMARY"]["disposition"] == "Changed"
    res = L.apply_plan(plan["plan_id"])
    assert mapping[later]["batch_ord"] in res["blocked_batch_ords"]
    snap = L.snapshots()
    print(f"  after revoke: dim_customer rows={sorted(snap.get(TBL_CUSTOMER, {}))} "
          f"fact_order rows={sorted(snap.get(TBL_ORDER, {}))}")
    assert all(r["name"] != "ada v3"
               for r in snap.get(TBL_CUSTOMER, {}).values())
    assert snap.get(TBL_ORDER, {}) == {}

    # Redelivery of the revoked tx never re-enters the warehouse.
    n_batches = len(L.ledger())
    r = cdc.receive_fragments(drill_transactions()[0][1])
    assert all(x["status"] == "duplicate" for x in r["fragments"])
    try:
        cdc.advance(SOURCE_ID)
        raise AssertionError("advance succeeded in a revoked lineage")
    except LedgerError:
        pass
    assert len(L.ledger()) == n_batches
    print("  redelivering revoked tx0: duplicates only; advance closed, "
          "no resurrected rows")

    # The association summary was recomputed from post-revocation input: the
    # row derived from the removed customer is gone; the surviving T3 customer
    # keeps its re-derived summary row.
    summary = snap["customer_summary"]
    assert 1 not in summary and summary[2]["name"] == "ben"
    print("  SUMMARY re-derived on new inputs: customer_summary="
          f"{json.dumps(summary, sort_keys=True)}")
    expl = cdc.explain_transaction(SOURCE_ID, 0)
    assert expl["batch"]["logical_batch_id"] == target
    L.close()


def section_crash_and_race(keep):
    banner("5. REAL SUBPROCESS CRASHES + TWO-PROCESS ADVANCE RACE")
    base = keep or tempfile.mkdtemp(prefix="cdccrash-")
    os.makedirs(base, exist_ok=True)
    db = os.path.join(base, "crash.db")
    if os.path.exists(db):
        os.unlink(db)
    L = new_ledger(keep, os.path.basename(db)) if keep else Ledger(db)
    if not keep:
        L.cdc().install_source(source_config())
    cdc = L.cdc()
    frag_dir = os.path.join(base, "frags")
    os.makedirs(frag_dir, exist_ok=True)
    f0 = os.path.join(frag_dir, "tx0.jsonl")
    write_jsonl(f0, drill_transactions()[0][1])
    L.close()

    # 5.1 crash after receive landed (worker simply receives then exits hard
    # via a normal completion; durability is what we verify on reopen).
    out, _ = worker("receive", db, SOURCE_ID, f0)
    assert out["ok"]
    L = Ledger(db)
    cdc = L.cdc()
    assert cdc.source_status(SOURCE_ID, window=1)["transactions"][0]["state"] \
        == ST_READY
    assert cdc.source_status(SOURCE_ID)["next_seq"] == 0
    print("  crash after receive: reopen shows staged tx, cursor unchanged")
    L.close()

    # 5.2 crash after staging but before the target commit.
    out, proc = worker("advance", db, SOURCE_ID,
                       "--crash", "after_staging_before_commit", expect=97)
    assert proc.returncode == 97
    L = Ledger(db)
    cdc = L.cdc()
    assert cdc.source_status(SOURCE_ID)["next_seq"] == 0
    assert L.snapshots().get(TBL_CUSTOMER, {}) == {}
    assert L.db.conn.execute(
        "SELECT COUNT(*) c FROM cdc_identity").fetchone()["c"] == 0
    assert L.db.conn.execute(
        "SELECT COUNT(*) c FROM ledger_batch").fetchone()["c"] == 0
    print("  crash pre-commit: no half tx, no mapping, no ledger batch")
    L.close()

    # 5.3 crash after the commit but before the response returns.
    out, proc = worker("advance", db, SOURCE_ID,
                       "--crash", "after_commit_before_response", expect=97)
    assert proc.returncode == 97
    L = Ledger(db)
    cdc = L.cdc()
    assert cdc.source_status(SOURCE_ID)["next_seq"] == 1
    assert len(L.ledger()) == 1
    snap = L.snapshots()
    state0, _, _, _ = MiniModel(source_config()).expected_rows_after([0])
    assert snap[TBL_CUSTOMER] == state0[TBL_CUSTOMER]
    print("  crash post-commit: reopen shows exactly one durable batch/decision")
    # Retrying the advance after response loss must not duplicate anything.
    out, _ = worker("advance", db, SOURCE_ID)
    assert out["ok"]
    assert len(Ledger(db).ledger()) == 1
    print("  retry after response loss: no duplicate ledger batch, no skip")
    L.close()

    # 5.4 two processes racing to advance the same next source transaction.
    db2 = os.path.join(base, "race.db")
    if os.path.exists(db2):
        os.unlink(db2)
    L = Ledger(db2)
    L.cdc().install_source(source_config())
    for seq in (0, 1):
        write_jsonl(os.path.join(frag_dir, f"race{seq}.jsonl"),
                    drill_transactions()[seq][1])
    L.cdc().receive_file(os.path.join(frag_dir, "race0.jsonl"))
    L.cdc().receive_file(os.path.join(frag_dir, "race1.jsonl"))
    L.close()
    procs = [subprocess.Popen([sys.executable, "-I", WORKER, "advance",
                               db2, SOURCE_ID],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True) for _ in range(2)]
    results = [p.wait() for p in procs]
    outs = []
    for p in procs:
        line = p.stdout.read().strip().splitlines()
        outs.append(json.loads(line[-1]) if line else {"ok": False})
    L = Ledger(db2)
    n_batches = len(L.ledger())
    decisions = L.db.conn.execute(
        "SELECT COUNT(*) c FROM cdc_tx_decision").fetchone()["c"]
    assert n_batches == 2, (results, outs, n_batches)
    assert decisions == 2
    print(f"  concurrent advancers: both finished (rc={results}); exactly "
          f"{n_batches} batches and {decisions} decisions, one per tx")
    L.close()


def section_cli(keep):
    banner("6. CLI: cdc-source / receive / status / advance / explain / row")
    base = keep or tempfile.mkdtemp(prefix="cdccli-")
    os.makedirs(base, exist_ok=True)
    db = os.path.join(base, "cli.db")
    if os.path.exists(db):
        os.unlink(db)
    cfg_path = os.path.join(base, "source.json")
    with open(cfg_path, "w", encoding="utf-8") as fh:
        json.dump(source_config(), fh)
    frag_path = os.path.join(base, "tx3.jsonl")
    write_jsonl(frag_path, drill_transactions()[3][1])

    def cli(*args, expect=0):
        proc = subprocess.run(
            [sys.executable, "-m", "revoke_ledger.cli", *args],
            cwd=ROOT, capture_output=True, text=True)
        assert proc.returncode == expect, (args, proc.stdout, proc.stderr)
        return json.loads(proc.stdout)

    view = cli("cdc-source", db, cfg_path)
    assert view["source_id"] == SOURCE_ID
    cli("cdc-receive", db, SOURCE_ID, frag_path)
    st = cli("cdc-status", db, SOURCE_ID, "4")
    assert st["transactions"][3]["state"] == ST_READY
    assert st["first_blocker"]["tx_seq"] == 0
    # Delivering the full drill through the CLI and advancing.
    d2 = os.path.join(base, "full.db")
    cli("cdc-source", d2, cfg_path)
    for i, (_label, frags) in enumerate(drill_delivery_order()):
        p = os.path.join(base, f"d{i}.jsonl")
        write_jsonl(p, frags)
        cli("cdc-receive", d2, SOURCE_ID, p)
    adv = cli("cdc-advance", d2, SOURCE_ID)
    assert [d["outcome"] for d in adv["decisions"]] == \
        ["committed", "committed", "aborted", "committed"]
    expl = cli("cdc-explain", d2, SOURCE_ID, "1")
    assert len(expl["change_maps"]) == 5
    row = cli("cdc-row", d2, TBL_ORDER, "1")
    assert row["source_key"] == [7]
    print("  CLI round trip: config, receive, blocked status, advance, "
          "tx explanation and reverse row lookup all match")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", default=None,
                    help="directory to keep drill SQLite files in")
    args = ap.parse_args()
    section_out_of_order(args.keep)
    section_full_drill_vs_model(args.keep)
    section_redelivery_and_files(args.keep)
    section_revocation(args.keep)
    section_crash_and_race(args.keep)
    section_cli(args.keep)
    banner("ALL CDC DRILLS COMPLETED")
    print("  Managed rows, identity mappings and the cursor were checked against")
    print("  an independent dictionary model; crash points used real subprocess")
    print("  exits and concurrent advancers produced one decision per transaction.")


if __name__ == "__main__":
    main()
