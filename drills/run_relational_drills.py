"""Executable drills for managed-relational/v1 (入仓加工: 订单 -> 渠道归属 -> 客户汇总).

    python3 drills/run_relational_drills.py
    python3 drills/run_relational_drills.py --keep /tmp/rkeep

Every expected join pairing, group membership and final managed row below is
hand-computed from the fixtures (independent enumeration in
fixtures/relational_scenarios.py), never produced by the product's relational
operators.  The assertions check pairings and amounts, not just row counts.
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

from fixtures import relational_scenarios as rs  # noqa: E402
from revoke_ledger.errors import ProtocolError  # noqa: E402
from revoke_ledger.ledger import Ledger  # noqa: E402
from revoke_ledger.rprotocol import validate_relational_program  # noqa: E402


def banner(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def show_state(label, L, tables):
    print(f"  {label}:")
    snap = L.snapshots()
    for t in tables:
        rows = snap.get(t, {})
        if rows:
            print(f"    {t}: " + ", ".join(
                f"[{rid}]={json.dumps(row, sort_keys=True)}"
                for rid, row in sorted(rows.items())))
        else:
            print(f"    {t}: (empty)")


def src_ids(identity):
    """Terminal source row_ids inside a (nested) row identity."""
    out = []

    def walk(x):
        if isinstance(x, list):
            if x and x[0] == "src":
                out.append(x[2])
                return
            for y in x:
                walk(y)
    walk(identity)
    return sorted(out)


def join_pairs(evidence, alias):
    rel = [r for r in evidence["relations"]
           if r["op"] == "join" and r["alias"] == alias][0]
    d = rel["detail"]
    return (sorted((tuple(src_ids(p["left"])), tuple(src_ids(p["right"])))
                   for p in d["pairs"]),
            sorted(tuple(src_ids(u)) for u in d["unmatched_left"]),
            sorted(tuple(src_ids(u)) for u in d["unmatched_right"]))


def group_view(evidence, alias):
    rel = [r for r in evidence["relations"]
           if r["op"] == "group" and r["alias"] == alias][0]
    return rel["detail"]["groups"]


def new_ledger(keep, name, seed, rules=None):
    if keep:
        os.makedirs(keep, exist_ok=True)
        path = os.path.join(keep, name)
        if os.path.exists(path):
            os.unlink(path)
    else:
        path = os.path.join(tempfile.mkdtemp(prefix="rdrill-"), name)
    L = Ledger(path)
    if rules is not None:
        L.install_constraints(rules)
    L.seed(seed)
    return L


def section_protocol(_keep):
    banner("0. PROTOCOL: VERSIONED JSON, CLOSED VALIDATION, STATIC FIELD CHECKS")
    p = validate_relational_program(rs.prog_settle_inner())
    print(f"  schema={p['schema']}  steps={len(p['steps'])}  "
          f"digest input is inert JSON (no SQL/code/network)")
    bad = rs.prog_settle_inner()
    bad["steps"][3]["select"] = [{"from": "left.amount", "as": "x"},
                                 {"from": "left.oid", "as": "x"}]
    try:
        validate_relational_program(bad)
        raise SystemExit("ERROR: name conflict accepted")
    except ProtocolError as exc:
        print(f"  projection name conflict -> rejected: {str(exc)[:72]}")
    bad2 = rs.prog_settle_inner()
    bad2["steps"][4]["aggregates"] = [{"op": "sum", "field": "nosuch",
                                       "as": "t"}]
    try:
        validate_relational_program(bad2)
        raise SystemExit("ERROR: undefined field accepted")
    except ProtocolError as exc:
        print(f"  undefined field on closed schema -> rejected: {str(exc)[:72]}")
    bad3 = rs.prog_settle_inner()
    bad3["steps"][3]["select"] = [{"from": "amount", "as": "amount"}]
    try:
        validate_relational_program(bad3)
        raise SystemExit("ERROR: unqualified join field accepted")
    except ProtocolError as exc:
        print(f"  unqualified field on a join -> rejected: {str(exc)[:72]}")


def section_inner(keep):
    banner("1. SETTLE-INNER: 订单 ⋈ 渠道归属 (inner) -> 按客户汇总 -> 受管汇总行")
    L = new_ledger(keep, "inner.db", rs.settlement_seed())
    r = L.submit(rs.prog_settle_inner())
    ev = r["evidence"]
    print(f"  batch SETTLE-INNER -> {r['status']} gen={r['generation']}")
    pairs, unmatched_l, unmatched_r = join_pairs(ev, "j")
    want_pairs = sorted(((o,), (a,)) for o, a in rs.EXPECTED_INNER["pairs"])
    print(f"  join pairs (order,attribution): "
          f"{[(l[0], r_[0]) for l, r_ in pairs]}")
    print(f"  unmatched left orders: {[u[0] for u in unmatched_l]}  "
          f"(channel 'none'; TEXT '1' != INT 1)")
    assert pairs == want_pairs, (pairs, want_pairs)
    assert [u[0] for u in unmatched_l] == rs.EXPECTED_INNER["unmatched_left"]
    assert unmatched_r == []
    groups = group_view(ev, "g")
    print("  groups (customer <- members, n/total/lo/hi):")
    by_customer = {}
    for g in groups:
        cust = g["key"][0]["value"]
        members = sorted(tuple(src_ids(m)) for m in g["members"])
        by_customer[cust] = (members, g["values"])
        print(f"    {cust}: members={members} values={g['values']}")
    for cust, want in rs.EXPECTED_INNER["groups"].items():
        members, values = by_customer[cust]
        want_members = sorted((o, a) for o, a in want["pairs"])
        assert members == want_members, (cust, members, want_members)
        assert values == {"n": want["n"], "total": want["total"],
                          "lo": want["lo"], "hi": want["hi"]}, (cust, values)
    print("  one-to-many: order 1 (web,100) feeds BOTH customer 101 and 102;")
    print("  order 6 (INT channel 1) matches attribution 13 (INT 1) but order 5")
    print("  (TEXT '1') matches nothing.")
    show_state("managed customer_summary", L, ["customer_summary"])
    assert L.snapshots()["customer_summary"] == rs.EXPECTED_INNER["summary"]
    print("  hand-computed pairs/members/amounts == product evidence: OK")
    L.close()


def section_left(keep):
    banner("2. SETTLE-LEFT: 未匹配明细保留 (left join) -> 以原明细 row_id 落库")
    L = new_ledger(keep, "left.db", rs.settlement_seed())
    r = L.submit(rs.prog_settle_left())
    ev = r["evidence"]
    pairs, unmatched_l, unmatched_r = join_pairs(ev, "j")
    print(f"  matched pairs: {[(l[0], r_[0]) for l, r_ in pairs]}")
    print(f"  unmatched left (kept once, right side null): "
          f"{[u[0] for u in unmatched_l]}")
    assert pairs == sorted(((o,), (a,)) for o, a in rs.EXPECTED_LEFT["pairs"])
    assert [u[0] for u in unmatched_l] == rs.EXPECTED_LEFT["unmatched_left"]
    writes = [w for w in ev["writes"] if w["table"] == "order_enriched"][0]
    targets = sorted(row["target_row_id"] for row in writes["rows"])
    print(f"  write targets = original order row_ids: {targets}")
    assert targets == [1, 2, 3, 4, 5, 6]
    show_state("order_enriched", L, ["order_enriched"])
    assert L.snapshots()["order_enriched"] == rs.EXPECTED_LEFT["enriched"]
    print("  orders 4 and 5 are kept with customer=null (no attribution);")
    print("  target identity came from the declared oid field, not an index.")
    L.close()


def section_composite(keep):
    banner("3. SETTLE-COMPOSITE: 复合键中的类型与空值 (null/missing 不匹配)")
    L = new_ledger(keep, "composite.db", rs.settlement_seed())
    r = L.submit(rs.prog_settle_composite())
    ev = r["evidence"]
    pairs, unmatched_l, unmatched_r = join_pairs(ev, "j")
    print(f"  matched pairs (shipment,lane_owner): "
          f"{[(l[0], r_[0]) for l, r_ in pairs]}")
    print(f"  unmatched shipments: {[u[0] for u in unmatched_l]} "
          f"(region null / region missing)")
    print(f"  unmatched lane_owner rows: {[u[0] for u in unmatched_r]} "
          f"(region null: null never matches, not even null=null)")
    assert pairs == sorted(((s,), (l,)) for s, l in
                           rs.EXPECTED_COMPOSITE["pairs"])
    assert [u[0] for u in unmatched_l] == rs.EXPECTED_COMPOSITE["unmatched_left"]
    assert [u[0] for u in unmatched_r] == \
        rs.EXPECTED_COMPOSITE["unmatched_right"]
    show_state("shipment_enriched", L, ["shipment_enriched"])
    assert L.snapshots()["shipment_enriched"] == rs.EXPECTED_COMPOSITE["enriched"]
    L.close()


def section_chain(keep):
    banner("4. SETTLE-CHAIN: 先连接再连接再汇总 (join -> join -> group)")
    L = new_ledger(keep, "chain.db", rs.settlement_seed())
    r = L.submit(rs.prog_settle_chain())
    ev = r["evidence"]
    rels = {x["alias"]: x for x in ev["relations"]}
    print(f"  j1 (orders x attribution) rows={rels['j1']['row_count']}  "
          f"j2 (x customers) rows={rels['j2']['row_count']}")
    assert rels["j1"]["row_count"] == 6 and rels["j2"]["row_count"] == 6
    groups = group_view(ev, "g")
    got = {}
    for g in groups:
        cust, name = (c["value"] for c in g["key"])
        got[(cust, name)] = g["values"]
        members = sorted(tuple(src_ids(m)) for m in g["members"])
        print(f"    group {(cust, name)}: values={g['values']} "
              f"member sources={members}")
    want = {k: {"n": v["n"], "total": v["total"]}
            for k, v in rs.EXPECTED_CHAIN["groups"].items()}
    assert got == want, (got, want)
    show_state("customer_stats", L, ["customer_stats"])
    assert L.snapshots()["customer_stats"] == rs.EXPECTED_CHAIN["stats"]
    print("  composite group key (customer,name); joined relation joined again.")
    L.close()


def section_empty_and_cancel(keep):
    banner("5. SETTLE-EMPTY / SETTLE-CANCEL: 空输入全局聚合 与 精确正负抵消")
    L = new_ledger(keep, "empty.db", rs.settlement_seed())
    r = L.submit(rs.prog_settle_empty())
    ev = r["evidence"]
    g = group_view(ev, "g")[0]
    print(f"  empty input, no keys -> one global row: {g['values']}")
    assert g["members"] == []
    assert g["values"] == {"n": 0, "na": 0, "total": None,
                           "lo": None, "hi": None}
    gk = [x for x in ev["relations"] if x["alias"] == "gk"][0]
    print(f"  empty input, with keys -> {gk['row_count']} groups")
    assert gk["row_count"] == 0
    assert L.snapshots()["stats"][1] == rs.EXPECTED_EMPTY["stats"][1]
    r2 = L.submit(rs.prog_settle_cancel())
    total = L.snapshots()["stats"][2]["total"]
    print(f"  2**62 + 2**62 - 2**62 -> {total} "
          f"(intermediate 2**63 would overflow int64; final is representable)")
    assert r2["status"] == "success"
    assert total == rs.EXPECTED_CANCEL["total"]
    L.close()


def section_rejections(keep):
    banner("6. REJECTIONS: 溢出 / 目标碰撞 / 聚合类型错误 -> 整批不提交")
    L = new_ledger(keep, "reject.db", rs.settlement_seed())
    head = L.head()
    r = L.submit(rs.prog_settle_overflow())
    d = r["abort"]["detail"]
    print(f"  SETTLE-OVERFLOW -> {r['status']} "
          f"step={r['abort']['step_index']} aggregate={d['aggregate']!r} "
          f"group_key={d['group_key']} result={d['result']} (> int64 max)")
    assert r["status"] == "failed" and r["abort"]["code"] == "type_error"
    r = L.submit(rs.prog_settle_collide())
    d = r["abort"]["detail"]
    print(f"  SETTLE-COLLIDE  -> {r['status']} duplicate targets "
          f"{d['duplicates']} from {len(d['sources'])} derived rows "
          f"(identical content is still rejected)")
    assert r["status"] == "failed" and r["abort"]["code"] == "pk_conflict"
    r = L.submit(rs.prog_settle_typeerr())
    d = r["abort"]["detail"]
    print(f"  SETTLE-TYPEERR  -> {r['status']} step={r['abort']['step_index']} "
          f"aggregate={d['aggregate']!r} offending={d['offending']}")
    assert r["status"] == "failed" and r["abort"]["code"] == "type_error"
    assert L.head() == head
    assert L.snapshots().get("stats", {}) == {}
    print("  head unchanged; zero rows committed by any rejected batch.")


def section_constraints(keep):
    banner("7. TERMINAL CONSTRAINTS: 关系批写入仍走现有批末唯一键/外键/级联检查")
    L = new_ledger(keep, "constrained.db", rs.settlement_seed(),
                   rules=rs.SETTLEMENT_RULES)
    r = L.submit(rs.prog_settle_inner())
    assert r["status"] == "success"
    print(f"  SETTLE-INNER on a constrained ledger -> {r['status']}")
    head = L.head()
    r = L.submit(rs.prog_settle_fk_violation())
    print(f"  SETTLE-FK -> {r['status']} ({r['abort']['code']})")
    for reason in r["abort"]["reasons"]:
        print(f"    {reason['code']}: audit_summary[{reason['row_id']}]."
          f"{reason['field']} -> customers[{reason['referenced_row_id']}]")
    assert r["status"] == "failed"
    assert r["abort"]["code"] == "constraint_violation"
    r = L.submit(rs.prog_settle_unique_violation())
    print(f"  SETTLE-UNIQUE -> {r['status']} "
          f"({r['abort']['reasons'][0]['code']} on ticket 'T-1')")
    assert r["abort"]["reasons"][0]["code"] == "unique_violation"
    assert L.head() == head
    assert L.snapshots().get("audit_summary", {}) == {}
    assert L.snapshots().get("order_enriched", {}) == {}
    print("  both rejected at the batch-final gate; no unconstrained import "
          "path exists.")
    L.close()


def section_revocation(keep):
    banner("8. REVOCATION: 右行恢复 -> 汇总重推导 / guard 阻塞 / 无关分区跳过")
    L = new_ledger(keep, "revocation.db", rs.revocation_seed())
    for b in rs.revocation_batches():
        r = L.submit(b)
        print(f"  run {b['logical_batch_id']:<5} -> {r['status']}")
    print("  T deleted attribution[11] (app -> customer 102).")
    show_state("before revocation", L,
               ["customer_summary", "order_enriched", "stats", "gamma"])
    orig = L.explain_derived("PROC")
    jl0 = [j for j in orig["joins"] if j["alias"] == "jl"][0]
    print(f"  original PROC jl unmatched left orders: "
          f"{[u[2] for u in jl0['unmatched_left']]} (order 3 null-padded)")
    assert [u[2] for u in jl0["unmatched_left"]] == [3]

    plan = L.plan_revocation("rev-rel", "T")
    print("  disposition by batch (original order):")
    for m in plan["mapping"]:
        print(f"    #{m['batch_ord']:<2} {m['logical_batch_id']:<5} "
              f"{m['disposition']:<11} {m['reason_code']:<28} "
              f"{m.get('reuse_kind')}")
    m = {x["logical_batch_id"]: x for x in plan["mapping"]}
    elig = plan["skip_eligibility"][str(m["PROC"]["batch_ord"])]
    print(f"  PROC skip eligibility: {elig['eligible']} - {elig['why'][0]}")
    assert not elig["eligible"]
    assert m["PROC"]["disposition"] == "Changed"
    assert m["G"]["reason_code"] == "blocked_guard"
    assert m["Z"]["reuse_kind"] == "skipped_independent"

    import revoke_ledger.ledger as _lg
    orig_exec = _lg.execute_program
    executed = []

    def counting(program, state, constraints=None):
        executed.append(program["logical_batch_id"])
        if constraints is None:
            return orig_exec(program, state)
        return orig_exec(program, state, constraints=constraints)

    _lg.execute_program = counting
    try:
        res = L.apply_plan(plan["plan_id"])
    finally:
        _lg.execute_program = orig_exec
    print(f"  interpreter actually invoked for: {executed}")
    assert executed == ["PROC", "G"], executed
    print(f"  replayed={res['replayed_batch_ords']} "
          f"skipped={res['skipped_independent_batch_ords']} "
          f"blocked={res['blocked_batch_ords']} {res['blocked_reasons']}")
    trace = L.db.generation_info(res["new_generation"])["trace"]["events"][0]
    print(f"  trace: executed={trace['executed_batch_ords']} "
          f"skipped={trace['skipped_batch_ords']} "
          f"blocked={trace['blocked_batch_ords']}")
    assert m["Z"]["batch_ord"] in trace["skipped_batch_ords"]
    assert m["Z"]["batch_ord"] not in trace["executed_batch_ords"]

    snap = L.snapshots()
    show_state("after revocation", L,
               ["customer_summary", "order_enriched", "stats", "gamma"])
    assert snap["customer_summary"] == rs.EXPECTED_REV_AFTER["customer_summary"]
    assert snap["order_enriched"] == rs.EXPECTED_REV_AFTER["order_enriched"]
    assert snap.get("stats", {}) == rs.EXPECTED_REV_AFTER["stats"]
    assert snap["gamma"] == rs.EXPECTED_REV_AFTER["gamma"]
    print("  attribution[11] restored -> order 3 now matches; summary for")
    print("  customer 102 appears (re-derived, not patched); G's guard")
    print("  int_sum(total) <= 150 fails on 220 -> G blocked, stats unwritten;")
    print("  Z (v1 partition) never entered the interpreter.")

    derived = m["PROC"]["detail"]["replay_derived"]
    jl1 = [x for x in derived if x["alias"] == "jl"][0]
    got_pairs = sorted((p["left"][2], p["right"][2])
                       for p in jl1["detail"]["pairs"])
    print(f"  new-generation jl pairs: {got_pairs} "
          f"unmatched={jl1['detail']['unmatched_left']}")
    assert got_pairs == rs.EXPECTED_REV_AFTER["proc_left_pairs"]
    assert jl1["detail"]["unmatched_left"] == []

    print("  reopen the database: derived explanations read back identically")
    path = L.db.path
    L.close()
    L2 = Ledger(path)
    again = L2.explain_derived("PROC")
    assert again == orig
    plan2 = L2.plan_view(plan["plan_id"])
    m2 = {x["logical_batch_id"]: x for x in plan2["mapping"]}
    assert m2["PROC"]["detail"]["replay_derived"] == derived
    expl = L2.explain_revocation("rev-rel")
    assert expl["batches"]["PROC"]["derived_relations"] == derived
    print(f"  original evidence (unmatched [3]) and new-generation derived")
    print(f"  evidence (pairs {got_pairs}) are stored separately, both intact.")
    L2.close()


def section_cli(keep):
    banner("9. CLI: init/ingest/run/explain-derived/show over committed JSON")
    base = keep or tempfile.mkdtemp(prefix="rdrill-cli-")
    os.makedirs(base, exist_ok=True)
    db = os.path.join(base, "cli.db")
    if os.path.exists(db):
        os.unlink(db)
    fx = os.path.join(ROOT, "fixtures", "relational")

    def cli(*args, expect=0):
        proc = subprocess.run(
            [sys.executable, "-m", "revoke_ledger.cli", *args],
            cwd=ROOT, capture_output=True, text=True)
        if expect == 0:
            assert proc.returncode == 0, (args, proc.stdout, proc.stderr)
        else:
            assert proc.returncode == expect, (args, proc.returncode,
                                               proc.stdout)
        # Some commands (init with constraints) print more than one document.
        out, idx, dec = [], 0, json.JSONDecoder()
        text = proc.stdout
        while idx < len(text):
            while idx < len(text) and text[idx].isspace():
                idx += 1
            if idx >= len(text):
                break
            obj, idx = dec.raw_decode(text, idx)
            out.append(obj)
        return out[-1] if out else None

    out = cli("init", db, os.path.join(fx, "seed.json"),
              os.path.join(fx, "constraints.json"))
    print(f"  init        -> generation {out['generation']} (seed committed)")
    out = cli("ingest", db, os.path.join(fx, "settle_inner.json"))
    print(f"  ingest      -> batch_ord {out['batch_ord']} "
          f"hash={out['program_hash'][:20]}...")
    out = cli("run", db, "SETTLE-INNER")
    print(f"  run         -> {out['status']} gen={out['generation']}")
    assert out["status"] == "success"
    out = cli("explain-derived", db, "SETTLE-INNER")
    pairs = [(p["left"][2], p["right"][2]) for p in out["joins"][0]["pairs"]]
    print(f"  explain     -> join pairs {pairs}")
    assert pairs == rs.EXPECTED_INNER["pairs"]
    totals = {g["key"][0]["value"]: g["values"]["total"]
              for g in out["groups"][0]["groups"]}
    print(f"  group totals per customer: {totals}")
    assert totals == {c: v["total"]
                      for c, v in rs.EXPECTED_INNER["groups"].items()}
    out = cli("ingest", db, os.path.join(fx, "settle_left.json"))
    out = cli("run", db, "SETTLE-LEFT")
    assert out["status"] == "success"
    out = cli("show", db)
    summary = {int(k): v for k, v in out["customer_summary"].items()}
    enriched = {int(k): v for k, v in out["order_enriched"].items()}
    print(f"  show        -> customer_summary={json.dumps(summary, sort_keys=True)}")
    print(f"               order_enriched has {len(enriched)} rows "
          f"(2 unmatched with customer=null)")
    assert summary == rs.EXPECTED_INNER["summary"]
    assert enriched == rs.EXPECTED_LEFT["enriched"]
    out = cli("ingest", db, os.path.join(fx, "settle_fk.json"))
    out = cli("run", db, "SETTLE-FK")
    print(f"  run SETTLE-FK -> {out['status']} ({out['abort']['code']})")
    assert out["status"] == "failed"
    out = cli("ingest", db, os.path.join(fx, "settle_collide.json"))
    out = cli("run", db, "SETTLE-COLLIDE")
    print(f"  run SETTLE-COLLIDE -> {out['status']} ({out['abort']['code']})")
    assert out["status"] == "failed"
    print("  CLI round trip matches the hand-computed expectations.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", default=None,
                    help="directory to keep drill SQLite files in")
    args = ap.parse_args()

    section_protocol(args.keep)
    section_inner(args.keep)
    section_left(args.keep)
    section_composite(args.keep)
    section_chain(args.keep)
    section_empty_and_cancel(args.keep)
    section_rejections(args.keep)
    section_constraints(args.keep)
    section_revocation(args.keep)
    section_cli(args.keep)

    banner("ALL RELATIONAL DRILLS COMPLETED")
    print("  Join pairings, group members and managed rows were asserted against")
    print("  hand-computed expectations (independent enumeration), never against")
    print("  the product's own operators.")


if __name__ == "__main__":
    main()
