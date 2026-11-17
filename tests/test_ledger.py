"""Unit and integration tests (stdlib unittest; run from /workspace).

    python3 -m unittest discover -s tests -v
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import oracle
import reference_interpreter as ref
from fixtures.scenarios import (prog, _read, _write, main_drill, guard_drill,
                                empty_guard_drill, predicate_range_drill,
                                overwrite_same_id_drill, multi_step_failure_drill,
                                multi_step_failure_success_drill)
from revoke_ledger.errors import (BatchAbort, LedgerError, ProtocolError,
                                  StaleError)
from revoke_ledger.ledger import Ledger
from revoke_ledger.protocol import validate_program
from revoke_ledger.store import ManagedDB


def new_db():
    d = tempfile.mkdtemp(prefix="revtest-")
    return Ledger(os.path.join(d, "warehouse.db"))


def run_all(ledger, batches):
    out = []
    for b in batches:
        out.append(ledger.submit(b))
    return out


def mapping_by_name(result):
    return {m["logical_batch_id"]: m for m in result["mapping"]}


class Base(unittest.TestCase):
    def assert_matches_oracle(self, ledger, target_ord):
        cmp = oracle.compare_oracle(ledger.db, target_ord)
        self.assertTrue(cmp["match"], json.dumps(cmp["diffs"], indent=1))
        return cmp


# ---------------------------------------------------------------------------
class TestProtocol(Base):
    def valid(self):
        return {"logical_batch_id": "p", "schema": "managed-batch/v1", "steps": [
            _read("r", "t", [1], {"kind": "all"}),
            _write("insert", "t", "r", [
                {"source_row_id": 1, "row_id": {"const": 2},
                 "values": {"a": {"const": 1}}}])]}

    def test_valid_roundtrip(self):
        p = validate_program(self.valid())
        self.assertEqual(p["steps"][1]["rows"][0]["source_row_id"], 1)

    def test_rejects_unknown_toplevel_key(self):
        p = self.valid()
        p["sql"] = "DROP TABLE x"
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_unknown_op(self):
        p = self.valid()
        p["steps"].append({"op": "exec", "sql": "select 1"})
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_unknown_predicate(self):
        p = self.valid()
        p["steps"][0]["select"]["where"] = {"kind": "like", "field": "a", "value": "x"}
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_non_int_range_bounds(self):
        p = self.valid()
        p["steps"][0]["select"]["where"] = {"kind": "range", "field": "a",
                                            "lo": 0.5, "hi": 2}
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_forward_alias(self):
        p = self.valid()
        p["steps"][0]["select"]["where"] = {"kind": "eq", "field": "later.a",
                                            "value": 1}
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_source_outside_map(self):
        p = self.valid()
        p["steps"][1]["rows"][0]["source_row_id"] = 99
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_duplicate_map_identity(self):
        p = self.valid()
        p["steps"][0]["map"].append({"row_id": 1, "table": "t"})
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_current_time_and_random(self):
        p = self.valid()
        p["steps"][1]["rows"][0]["values"] = {"a": {"const": 0},
                                              "b": {"now": []}}
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_arbitrary_value_op(self):
        p = self.valid()
        p["steps"][1]["rows"][0]["values"] = {"a": {"mul": [{"const": 1},
                                                            {"const": 2}]}}
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_int_overflow_in_id_expr(self):
        p = self.valid()
        p["steps"][1]["rows"][0]["row_id"] = {"add": [{"const": 2 ** 63 - 1}, 1]}
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_noncontiguous_partition_ranges(self):
        p = self.valid()
        p["partition"] = {"table": "t", "ranges": [[5, 9], [1, 2]]}
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_guard_without_bound(self):
        p = self.valid()
        p["steps"].append({"op": "guard", "test": "row_count", "alias": "r"})
        with self.assertRaises(ProtocolError):
            validate_program(p)

    def test_rejects_row_count_guard_with_field(self):
        p = self.valid()
        p["steps"].append({"op": "guard", "test": "row_count", "alias": "r",
                           "field": "a", "eq": 1})
        with self.assertRaises(ProtocolError):
            validate_program(p)


# ---------------------------------------------------------------------------
class TestExecution(Base):
    def setUp(self):
        self.L = new_db()
        self.L.seed({"t": {1: {"a": 5, "s": "x", "b": True, "n": None},
                           2: {"a": 50, "s": None, "b": False, "n": 7}},
                     "u": {}})

    def test_int64_text_bool_null_roundtrip(self):
        snap = self.L.snapshots()["t"]
        self.assertEqual(snap[1], {"a": 5, "s": "x", "b": True, "n": None})
        self.assertEqual(snap[2], {"a": 50, "s": None, "b": False, "n": 7})

    def test_null_semantics_eq_range_is_null_not(self):
        L = self.L
        r = L.submit(prog("q", [
            _read("r", "t", [1, 2], {"kind": "not", "arg": {"kind": "eq",
                                                            "field": "n",
                                                            "value": 7}})]))
        self.assertEqual(r["status"], "success")
        sel = r["evidence"]["reads"][0]["selected_ids"]
        # null never equals 7 -> NOT is true for row1(null) and row2 has 7 -> false
        self.assertEqual(sel, [1])

    def test_range_excludes_null(self):
        L = self.L
        r = L.submit(prog("rng", [
            _read("r", "t", [1, 2], {"kind": "range", "field": "n",
                                     "lo": -100, "hi": 100})]))
        self.assertEqual(r["evidence"]["reads"][0]["selected_ids"], [2])

    def test_bool_not_int(self):
        # True must not equal 1 in eq
        L = self.L
        r = L.submit(prog("b", [
            _read("r", "t", [1, 2], {"kind": "eq", "field": "b", "value": 1})]))
        self.assertEqual(r["evidence"]["reads"][0]["selected_ids"], [])

    def test_pk_conflict_fails_batch_and_writes_nothing(self):
        L = self.L
        r = L.submit(prog("pk", [
            _read("r", "t", [1], {"kind": "all"}),
            _write("replace", "u", "r", [
                {"source_row_id": 1, "row_id": {"const": 9},
                 "values": {"a": {"const": 1}}}]),
            _write("insert", "t", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"a": {"const": 1}}}]),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "pk_conflict")
        self.assertIsNone(r["generation"])
        self.assertEqual(L.snapshots()["t"][1]["a"], 5)
        self.assertNotIn(9, L.snapshots().get("u", {}))

    def test_guard_failure_no_generation(self):
        L = self.L
        gen_before = L.head()["generation"]
        r = L.submit(prog("g", [
            _read("r", "t", [1], {"kind": "all"}),
            {"op": "guard", "test": "int_sum", "alias": "r", "field": "a",
             "ge": 100},
            _write("replace", "t", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"a": {"const": 1}}}])]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "guard_failed")
        self.assertEqual(L.head()["generation"], gen_before)

    def test_ingest_immutable_program(self):
        L = self.L
        p = prog("same", [_read("r", "t", [1], {"kind": "all"})])
        L.ingest(p)
        p2 = prog("same", [_read("r", "t", [2], {"kind": "all"})])
        with self.assertRaises(LedgerError):
            L.ingest(p2)
        # identical bytes are idempotent
        self.assertTrue(L.ingest(p)["already_ingested"])

    def test_time_order_enforced(self):
        L = self.L
        L.ingest(prog("first", [_read("r", "t", [1], {"kind": "all"})]))
        L.ingest(prog("second", [_read("r", "t", [1], {"kind": "all"})]))
        with self.assertRaises(LedgerError):
            L.run_ingested("second")
        L.run_ingested("first")
        L.run_ingested("second")

    def test_original_evidence_read_only(self):
        L = self.L
        L.submit(prog("a", [_read("r", "t", [1], {"kind": "all"})]))
        with self.assertRaises(sqlite3.IntegrityError):
            L.db.conn.execute("UPDATE ledger_batch SET status='failed'")
        with self.assertRaises(sqlite3.IntegrityError):
            L.db.conn.execute("DELETE FROM ledger_batch")
        with self.assertRaises(sqlite3.IntegrityError):
            L.db.conn.execute("UPDATE managed_rows SET row_json='{}' "
                              "WHERE generation=2")

    def test_cross_table_write(self):
        L = self.L
        r = L.submit(prog("x", [
            _read("r", "t", [1], {"kind": "all"}),
            _write("insert", "u", "r", [
                {"source_row_id": 1, "row_id": {"const": 77},
                 "values": {"copy": {"field": "a"}}}])]))
        self.assertEqual(r["status"], "success")
        self.assertEqual(L.snapshots()["u"][77], {"copy": 5})

    def test_exact_int_arith(self):
        L = self.L
        r = L.submit(prog("m", [
            _read("r", "t", [1], {"kind": "all"}),
            _write("replace", "t", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"a": {"sub": [{"field": "a"},
                                           {"const": 3}]}}}])]))
        self.assertEqual(r["status"], "success")
        self.assertEqual(L.snapshots()["t"][1]["a"], 2)


# ---------------------------------------------------------------------------
class TestMainDrill(Base):
    def test_main_drill(self):
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        pre = L.snapshots()
        self.assertEqual(pre["alpha"][1], {"amount": 20})
        self.assertEqual(pre["beta"][500], {"value": 40})
        self.assertEqual(pre["gamma"][100], {"value": 7})
        pre_head = L.head()

        plan = L.plan_revocation("rev-1", "T")
        # Planning must not move the committed generation.
        self.assertEqual(L.head(), pre_head)
        self.assertEqual(plan["phase"], "planned")

        m = mapping_by_name(plan)
        self.assertEqual(m["A"]["disposition"], "Kept")
        self.assertEqual(m["T"]["disposition"], "Superseded")
        self.assertEqual(m["B"]["disposition"], "Changed")
        self.assertEqual(m["C"]["disposition"], "Kept")
        self.assertEqual(m["C"]["reason_code"], "independent_partition")
        self.assertEqual(m["C"]["reuse_kind"], "skipped_independent")
        self.assertTrue(m["C"]["detail"]["actual_disjointness_proven"])

        # Diff identifies the target's own row and B's dependent row.
        diff = {(d["table"], d["row_id"]): d for d in plan["diff"]}
        self.assertEqual(diff[("alpha", 1)]["new"], '{"amount":10}')
        self.assertEqual(diff[("alpha", 1)]["cause"], "batch:2")
        self.assertEqual(diff[("beta", 500)]["old"], '{"value":40}')
        self.assertEqual(diff[("beta", 500)]["new"], '{"value":20}')
        self.assertEqual(diff[("beta", 500)]["cause"], "batch:3")

        res = L.apply_plan(plan["plan_id"])
        post = L.snapshots()
        self.assertEqual(post["alpha"][1], {"amount": 10})
        self.assertEqual(post["beta"][500], {"value": 20})   # not 40, not deleted
        self.assertEqual(post["gamma"][100], {"value": 7})
        self.assertEqual(res["skipped_independent_batch_ords"],
                         [m["C"]["batch_ord"]])
        self.assertNotIn(m["C"]["batch_ord"], res["replayed_batch_ords"])
        self.assert_matches_oracle(L, plan["target"]["batch_ord"])

        # Execution trace proves C was not executed and D (none) not run.
        trace = L.db.generation_info(res["new_generation"])["trace"]["events"][0]
        self.assertIn(m["C"]["batch_ord"], trace["skipped_batch_ords"])
        self.assertNotIn(m["C"]["batch_ord"], trace["executed_batch_ords"])
        self.assertEqual(trace["executed_batch_ords"], [m["B"]["batch_ord"]])

    def test_old_generation_and_evidence_preserved(self):
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        old_head = L.head()
        old_batch = L.db.batch_by_logical_id("C")
        plan = L.plan_revocation("rev-1", "T")
        L.apply_plan(plan["plan_id"])
        self.assertEqual(L.db.head()["generation"], old_head["generation"] + 1)
        # old generation still readable byte for byte
        self.assertEqual(L.db.get_row_at(old_head["generation"], "beta", 500),
                         {"value": 40})
        self.assertEqual(L.db.batch_by_logical_id("C"), old_batch)

    def test_one_revocation_per_target_and_no_rebind(self):
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        plan = L.plan_revocation("rev-1", "T")
        L.apply_plan(plan["plan_id"])
        with self.assertRaises(LedgerError):
            L.plan_revocation("rev-1", "B")       # rebind same id
        with self.assertRaises(LedgerError):
            L.plan_revocation("rev-2", "T")       # revoke same target twice
        with self.assertRaises(LedgerError):
            L.plan_revocation("rev-3", "T")
        # failed batches cannot be revoked
        L2 = new_db()
        L2.seed({"t": {1: {"a": 1}}})
        L2.submit(prog("f", [_read("r", "t", [1], {"kind": "all"}),
                             {"op": "guard", "test": "row_count", "alias": "r",
                              "eq": 9}]))
        with self.assertRaises(LedgerError):
            L2.plan_revocation("rev-f", "f")

    def test_post_target_ingest_after_revocation_closed(self):
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        plan = L.plan_revocation("rev-1", "T")
        L.apply_plan(plan["plan_id"])
        L.ingest(prog("later", [_read("r", "alpha", [1], {"kind": "all"})]))
        with self.assertRaises(LedgerError):
            L.run_ingested("later")


# ---------------------------------------------------------------------------
class TestGuardAndScope(Base):
    def test_later_batch_guard_blocks(self):
        L = new_db()
        initial, batches = guard_drill()
        L.seed(initial)
        run_all(L, batches)
        plan = L.plan_revocation("rev-g", "T")
        m = mapping_by_name(plan)
        self.assertEqual(m["G"]["disposition"], "Blocked")
        self.assertEqual(m["G"]["reason_code"], "blocked_guard")
        res = L.apply_plan(plan["plan_id"])
        self.assertIn(m["G"]["batch_ord"], res["blocked_batch_ords"])
        self.assertEqual(L.snapshots()["alpha"][1], {"amount": 10})
        self.assert_matches_oracle(L, plan["target"]["batch_ord"])

    def test_empty_read_guard_and_later_insert(self):
        L = new_db()
        initial, batches = empty_guard_drill()
        L.seed(initial)
        run_all(L, batches)
        self.assertEqual(L.snapshots()["probe"][9], {"amount": 1})
        plan = L.plan_revocation("rev-e", "T")
        m = mapping_by_name(plan)
        # E was originally failed -> stays a historical decision, never activated
        self.assertEqual(m["E"]["disposition"], "Blocked")
        self.assertEqual(m["E"]["reason_code"], "blocked_historical_failure")
        # Z originally inserted probe[9] only because amount>=20; now blocked
        self.assertEqual(m["Z"]["disposition"], "Blocked")
        self.assertEqual(m["Z"]["reason_code"], "blocked_guard")
        L.apply_plan(plan["plan_id"])
        self.assertEqual(L.snapshots(), {"alpha": {1: {"amount": 10}}})
        self.assert_matches_oracle(L, plan["target"]["batch_ord"])

    def test_predicate_range_membership_change(self):
        L = new_db()
        initial, batches = predicate_range_drill()
        L.seed(initial)
        run_all(L, batches)
        plan = L.plan_revocation("rev-p", "T")
        m = mapping_by_name(plan)
        self.assertEqual(m["W"]["disposition"], "Blocked")
        L.apply_plan(plan["plan_id"])
        # alpha[1] moved into the 10..35 window but W's guard requires exactly 1
        # selected and two are selected now, so it must be blocked, not run.
        self.assertNotIn(200, L.snapshots().get("beta", {}))
        self.assert_matches_oracle(L, plan["target"]["batch_ord"])

    def test_same_id_overwritten_by_later_batch_is_recomputed(self):
        L = new_db()
        initial, batches = overwrite_same_id_drill()
        L.seed(initial)
        run_all(L, batches)
        self.assertEqual(L.snapshots()["alpha"][1], {"amount": 21})
        plan = L.plan_revocation("rev-o", "T")
        self.assertEqual(mapping_by_name(plan)["B"]["disposition"], "Changed")
        L.apply_plan(plan["plan_id"])
        post = L.snapshots()
        self.assertEqual(post["alpha"][1], {"amount": 11})
        self.assertEqual(post["beta"][500], {"value": 10})
        self.assert_matches_oracle(L, plan["target"]["batch_ord"])


# ---------------------------------------------------------------------------
class TestPartitionAndSkip(Base):
    def test_skip_is_not_write_disjointness_only(self):
        """A declared partition outside the target's full key/scope domain skips."""
        L2 = new_db()
        L2.seed({"alpha": {1: {"amount": 10}, 2: {"amount": 4}}, "beta": {}})
        T = prog("T", [_read("r", "alpha", [1], {"kind": "all"}),
                       _write("replace", "alpha", "r", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"amount": {"const": 20}}}])])
        # D lives entirely on alpha[2], which the target neither writes nor
        # reads.  Note: key 2 is disjoint from the target's WRITE key 1 and its
        # READ scope {1}; the skip rests on the full read scope, not on write
        # set disjointness alone.
        D = prog("D", [_read("r", "alpha", [2], {"kind": "all"}),
                       _write("replace", "alpha", "r", [
                           {"source_row_id": 2, "row_id": {"const": 2},
                            "values": {"amount": {"const": 9}}}])],
                 partition={"table": "alpha", "ranges": [[2, 2]]})
        run_all(L2, [T, D])
        plan = L2.plan_revocation("rev-d", "T")
        m = mapping_by_name(plan)
        self.assertEqual(m["D"]["reuse_kind"], "skipped_independent")
        L2.apply_plan(plan["plan_id"])
        self.assertEqual(L2.snapshots()["alpha"],
                         {1: {"amount": 10}, 2: {"amount": 9}})
        self.assert_matches_oracle(L2, plan["target"]["batch_ord"])

    def test_partition_overlapping_read_scope_is_replayed(self):
        """If the partition intersects the target's *read scope*, no skip."""
        L = new_db()
        L.seed({"alpha": {1: {"amount": 10}, 2: {"amount": 4}}, "beta": {}})
        # Target reads alpha[1] and alpha[2] (scope includes both) but writes 1.
        T = prog("T", [_read("r", "alpha", [1, 2], {"kind": "all"}),
                       _write("replace", "alpha", "r", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"amount": {"const": 20}}}])])
        # D's partition covers key 2, which is inside the target read scope.
        D = prog("D", [_read("r", "alpha", [2], {"kind": "all"}),
                       _write("replace", "beta", "r", [
                           {"source_row_id": 2, "row_id": {"const": 2},
                            "values": {"value": {"const": 9}}}])],
                 partition={"table": "alpha", "ranges": [[2, 2]]})
        run_all(L, [T, D])
        plan = L.plan_revocation("rev-overlap", "T")
        m = mapping_by_name(plan)
        self.assertEqual(m["D"]["disposition"], "Kept")
        self.assertEqual(m["D"]["reuse_kind"], "replayed_identical")
        self.assertFalse(plan["skip_eligibility"][str(m["D"]["batch_ord"])]["eligible"])

    def test_declared_partition_not_covering_own_keys_is_ineligible(self):
        L = new_db()
        L.seed({"alpha": {1: {"amount": 10}}, "gamma": {1: {"v": 0}}})
        T = prog("T", [_read("r", "alpha", [1], {"kind": "all"}),
                       _write("replace", "alpha", "r", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"amount": {"const": 20}}}])])
        C = prog("C", [_read("g", "gamma", [1], {"kind": "all"}),
                       _write("replace", "gamma", "g", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"v": {"const": 7}}}])],
                 partition={"table": "gamma", "ranges": [[2, 2]]})
        run_all(L, [T, C])
        plan = L.plan_revocation("rev-nc", "T")
        m = mapping_by_name(plan)
        self.assertNotEqual(m["C"]["reuse_kind"], "skipped_independent")
        self.assertTrue(any("leaves partition" in w for w in
                            plan["skip_eligibility"][str(m["C"]["batch_ord"])]["why"]))

    def test_no_partition_never_skipped(self):
        L = new_db()
        L.seed({"alpha": {1: {"amount": 10}}, "gamma": {1: {"v": 0}}})
        T = prog("T", [_read("r", "alpha", [1], {"kind": "all"}),
                       _write("replace", "alpha", "r", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"amount": {"const": 20}}}])])
        C = prog("C", [_read("g", "gamma", [1], {"kind": "all"}),
                       _write("replace", "gamma", "g", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"v": {"const": 7}}}])])
        run_all(L, [T, C])
        plan = L.plan_revocation("rev-np", "T")
        m = mapping_by_name(plan)
        self.assertNotEqual(m["C"]["reuse_kind"], "skipped_independent")

    def test_skip_output_is_byte_identical(self):
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        old_c = L.db.batch_by_logical_id("C")
        old_gamma = L.db.get_row_at(old_c["generation"], "gamma", 100)
        plan = L.plan_revocation("rev-b", "T")
        res = L.apply_plan(plan["plan_id"])
        self.assertEqual(L.snapshots()["gamma"][100], old_gamma)
        self.assertEqual(L.snapshots()["gamma"][100], {"value": 7})
        self.assertIn(mapping_by_name(plan)["C"]["batch_ord"],
                      res["skipped_independent_batch_ords"])


# ---------------------------------------------------------------------------
class TestAtomicityAndStaleness(Base):
    def setUp(self):
        self.L = new_db()
        initial, batches = main_drill()
        self.L.seed(initial)
        run_all(self.L, batches)

    def test_stale_after_new_batch(self):
        L = self.L
        plan = L.plan_revocation("rev-s1", "T")
        L.submit(prog("X", [_read("r", "alpha", [2], {"kind": "all"}),
                            _write("replace", "alpha", "r", [
                                {"source_row_id": 2, "row_id": {"const": 2},
                                 "values": {"amount": {"const": 1}}}])]))
        with self.assertRaises(StaleError) as cm:
            L.apply_plan(plan["plan_id"])
        self.assertIn(cm.exception.code, ("head_moved", "fingerprint_changed"))
        # The new batch's effect is intact; nothing was overwritten.
        self.assertEqual(L.snapshots()["alpha"][2], {"amount": 1})

    def test_stale_after_ingest_only(self):
        L = self.L
        plan = L.plan_revocation("rev-s2", "T")
        L.ingest(prog("Y", [_read("r", "alpha", [1], {"kind": "all"})]))
        with self.assertRaises(StaleError) as cm:
            L.apply_plan(plan["plan_id"])
        self.assertEqual(cm.exception.code, "fingerprint_changed")

    def test_stale_after_external_edit(self):
        L = self.L
        plan = L.plan_revocation("rev-s3", "T")
        c = sqlite3.connect(L.db.path)
        c.execute("DROP TRIGGER managed_rows_no_update")
        c.execute("UPDATE managed_rows SET row_json='{\"value\":99}' "
                  "WHERE generation=6 AND tbl='beta' AND row_id=500")
        c.commit()
        c.close()
        with self.assertRaises(StaleError) as cm:
            L.apply_plan(plan["plan_id"])
        self.assertEqual(cm.exception.code, "fingerprint_changed")

    def test_stale_after_tampered_plan_payload(self):
        L = self.L
        plan = L.plan_revocation("rev-s4", "T")
        with L.db.tx():
            L.db.conn.execute(
                "UPDATE generation_plan SET plan_payload_json=? WHERE plan_id=?",
                ('{"target":{"batch_ord":2},"skip_set":[],"mapping":[]}',
                 plan["plan_id"]))
        with self.assertRaises(StaleError) as cm:
            L.apply_plan(plan["plan_id"])
        self.assertIn(cm.exception.code, ("plan_tampered", "replay_diverged",
                                          "state_diverged"))

    def test_stale_regular_commit_after_plan(self):
        """A normal successful batch after planning is a new head, not overwritten."""
        L = self.L
        plan = L.plan_revocation("rev-s5", "T")
        L.submit(prog("W2", [_read("r", "alpha", [1], {"kind": "all"}),
                             _write("replace", "alpha", "r", [
                                 {"source_row_id": 1, "row_id": {"const": 1},
                                  "values": {"amount": {"const": 12}}}])]))
        with self.assertRaises(StaleError):
            L.apply_plan(plan["plan_id"])
        self.assertEqual(L.snapshots()["alpha"][1], {"amount": 12})

    def test_response_lost_after_apply(self):
        L = self.L
        plan = L.plan_revocation("rev-r", "T")
        first = L.apply_plan(plan["plan_id"])
        # retry by revocation id / plan id: same committed decision
        r = L.revocation_result("rev-r")
        self.assertEqual(r["phase"], "applied")
        self.assertEqual(r["new_generation"], first["new_generation"])
        self.assertEqual(r["result"]["state_promise"], first["state_promise"])
        again = L.apply_plan(plan["plan_id"])
        self.assertEqual(again["new_generation"], first["new_generation"])
        self.assertEqual(L.head()["generation"], first["new_generation"])
        self.assertEqual(len([g for g in L.history()
                              if g["kind"] == "revocation"]), 1)

    def test_recovery_from_planning_stage(self):
        L = self.L
        plan = L.plan_revocation("rev-p", "T")
        with L.db.tx():
            L.db.conn.execute("UPDATE generation_plan SET phase='planning' "
                              "WHERE plan_id=?", (plan["plan_id"],))
            L.db.conn.execute("DELETE FROM revocation_mapping WHERE plan_id=? "
                              "AND batch_ord=4", (plan["plan_id"],))
        rec = L.recover_interrupted_plan("rev-p", "T")
        self.assertEqual(rec["action"], "replanned_from_stage")
        self.assertEqual(rec["plan"]["phase"], "planned")
        self.assertEqual(len(rec["plan"]["mapping"]), 4)
        L.apply_plan(rec["plan"]["plan_id"])
        self.assert_matches_oracle(L, 2)

    def test_crash_before_bind(self):
        rec = self.L.recover_interrupted_plan("rev-never", "T",
                                              crash_before_bind=True)
        self.assertEqual(rec["action"], "nothing_persisted")

    def test_readers_never_see_half_applied(self):
        """A second reader connection resolves the head then reads one generation."""
        L = self.L
        plan = L.plan_revocation("rev-atom", "T")
        reader = ManagedDB(L.db.path, create=False)
        before = reader.head()
        old = {(t, rid): row for t, rid, row in
               reader.rows_for_generation(before["generation"])}
        L.apply_plan(plan["plan_id"])
        after = reader.head()
        new = {(t, rid): row for t, rid, row in
               reader.rows_for_generation(after["generation"])}
        self.assertEqual(after["generation"], before["generation"] + 1)
        self.assertEqual(old[("alpha", 1)], {"amount": 20})
        self.assertEqual(new[("alpha", 1)], {"amount": 10})
        self.assertEqual(new[("beta", 500)], {"value": 20})
        reader.close()

    def test_generation_epoch_increments(self):
        L = self.L
        h0 = L.head()
        plan = L.plan_revocation("rev-e", "T")
        self.assertEqual(L.head(), h0)
        L.apply_plan(plan["plan_id"])
        h1 = L.head()
        self.assertEqual(h1["generation"], h0["generation"] + 1)
        self.assertEqual(h1["epoch"], h0["epoch"] + 1)


# ---------------------------------------------------------------------------
class TestOracleAgreement(Base):
    SCENARIOS = [
        ("main", main_drill),
        ("guard", guard_drill),
        ("empty", empty_guard_drill),
        ("predicate", predicate_range_drill),
        ("overwrite", overwrite_same_id_drill),
        ("multistep", multi_step_failure_drill),
        ("multistep_guard", multi_step_failure_success_drill),
    ]

    def test_every_scenario_matches_full_replay(self):
        for name, scn in self.SCENARIOS:
            with self.subTest(name=name):
                L = new_db()
                initial, batches = scn()
                L.seed(initial)
                run_all(L, batches)
                plan = L.plan_revocation("rev-" + name, "T")
                L.apply_plan(plan["plan_id"])
                cmp = self.assert_matches_oracle(L, plan["target"]["batch_ord"])
                # Product dispositions must be explainable by the oracle's
                # independent statuses.
                om = cmp["expected_outcomes"]
                for m in plan["mapping"]:
                    st = om[m["batch_ord"]]["status"]
                    if st == "Ran":
                        self.assertIn(m["disposition"], ("Kept", "Changed"))
                    elif st == "Blocked":
                        self.assertEqual(m["disposition"], "Blocked")
                    else:
                        self.assertEqual(m["disposition"], "Superseded")

    def test_independent_skip_equals_full_replay(self):
        """For main drill, C is skipped by the product but fully replayed by the
        oracle; both must produce the same gamma bytes and the same downstream
        state."""
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        plan = L.plan_revocation("rev-skip", "T")
        res = L.apply_plan(plan["plan_id"])
        m = mapping_by_name(plan)["C"]
        self.assertIn(m["batch_ord"], res["skipped_independent_batch_ords"])
        expected_rows, _ = oracle.oracle_replay(L.db, 2)
        self.assertEqual(L.snapshots()["gamma"], expected_rows["gamma"])


if __name__ == "__main__":
    unittest.main()


class TestAdversarialSkip(Base):
    def test_skip_demoted_when_changed_batch_changes_its_inputs(self):
        """A structurally eligible partition can still be affected indirectly:
        a Changed later batch writes a key inside its scope.  Skip must be
        withdrawn and the batch replayed."""
        L = new_db()
        L.seed({"alpha": {1: {"amount": 10}}, "gamma": {1: {"v": 0},
                                                       2: {"v": 0}}})
        T = prog("T", [_read("r", "alpha", [1], {"kind": "all"}),
                       _write("replace", "alpha", "r", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"amount": {"const": 20}}}])])
        B = prog("B", [_read("r", "alpha", [1], {"kind": "all"}),
                       _write("replace", "gamma", "r", [
                           {"source_row_id": 1, "row_id": {"const": 2},
                            "values": {"v": {"field": "amount"}}}])])
        C = prog("C", [_read("g", "gamma", [2], {"kind": "all"}),
                       _write("replace", "gamma", "g", [
                           {"source_row_id": 2, "row_id": {"const": 2},
                            "values": {"v": {"add": [{"field": "v"},
                                                      {"const": 1}]}}}])],
                 partition={"table": "gamma", "ranges": [[2, 2]]})
        run_all(L, [T, B, C])
        plan = L.plan_revocation("rev-adv", "T")
        m = mapping_by_name(plan)
        self.assertEqual(plan["skip_set"], [])
        self.assertEqual(m["C"]["disposition"], "Changed")
        L.apply_plan(plan["plan_id"])
        self.assertEqual(L.snapshots()["gamma"], {1: {"v": 0}, 2: {"v": 11}})
        self.assert_matches_oracle(L, plan["target"]["batch_ord"])

    def test_genuine_independent_skip_survives_demotion_check(self):
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        plan = L.plan_revocation("rev-keep", "T")
        m = mapping_by_name(plan)
        self.assertEqual(m["C"]["reuse_kind"], "skipped_independent")
        self.assertEqual(m["C"]["detail"]["actual_changed_keys_in_scope"], [])


class TestConcurrency(Base):
    def test_concurrent_apply_commits_once(self):
        import threading
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        plan = L.plan_revocation("rev-cc", "T")
        results = []

        def go(label):
            lg = Ledger(L.db.path)
            try:
                results.append(lg.apply_plan(plan["plan_id"])["new_generation"])
            except StaleError:
                results.append("stale")
            finally:
                lg.close()

        threads = [threading.Thread(target=go, args=(str(i),)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        rev = [g for g in L.history() if g["kind"] == "revocation"]
        self.assertEqual(len(rev), 1)
        self.assertEqual(L.head()["generation"], rev[0]["generation"])


class TestPartitionTableIntegrity(Base):
    def test_partition_table_must_be_the_read_write_table(self):
        L = new_db()
        L.seed({"alpha": {1: {"amount": 10}}, "gamma": {100: {"v": 0}},
                "beta": {}})
        T = prog("T", [_read("r", "alpha", [1], {"kind": "all"}),
                       _write("replace", "alpha", "r", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"amount": {"const": 20}}}])])
        # Declares a gamma partition but actually reads/writes gamma[100],
        # outside [200,299].  Must be ineligible on both counts.
        C = prog("C", [_read("g", "gamma", [100], {"kind": "all"}),
                       _write("replace", "gamma", "g", [
                           {"source_row_id": 100, "row_id": {"const": 100},
                            "values": {"v": {"const": 7}}}])],
                 partition={"table": "gamma", "ranges": [[200, 299]]})
        # A cross-table read under a partition on a different table.
        D = prog("D", [_read("b", "beta", [500], {"kind": "all"}),
                       _write("replace", "gamma", "b", [
                           {"source_row_id": 500, "row_id": {"const": 100},
                            "values": {"v": {"const": 1}}}])],
                 partition={"table": "gamma", "ranges": [[100, 100]]})
        run_all(L, [T, C])
        plan = L.plan_revocation("rev-pt", "T")
        el = plan["skip_eligibility"]
        m = mapping_by_name(plan)
        self.assertFalse(el[str(m["C"]["batch_ord"])]["eligible"])
        self.assertNotEqual(m["C"]["reuse_kind"], "skipped_independent")


class TestChainedDependency(Base):
    def test_later_batch_reading_changed_output_is_replayed(self):
        L = new_db()
        L.seed({"alpha": {1: {"amount": 10}}})
        T = prog("T", [_read("r", "alpha", [1], {"kind": "all"}),
                       _write("replace", "alpha", "r", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"amount": {"const": 20}}}])])
        B = prog("B", [_read("r", "alpha", [1], {"kind": "all"}),
                       _write("replace", "beta", "r", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"v": {"field": "amount"}}}])])
        C = prog("C", [_read("b", "beta", [1], {"kind": "all"}),
                       _write("replace", "beta", "b", [
                           {"source_row_id": 1, "row_id": {"const": 2},
                            "values": {"v": {"add": [{"field": "v"},
                                                      {"const": 100}]}}}])],
                 partition={"table": "beta", "ranges": [[1, 1]]})
        run_all(L, [T, B, C])
        plan = L.plan_revocation("rev-chain", "T")
        m = mapping_by_name(plan)
        self.assertEqual(plan["skip_set"], [])
        self.assertEqual(m["B"]["disposition"], "Changed")
        self.assertEqual(m["C"]["disposition"], "Changed")
        L.apply_plan(plan["plan_id"])
        self.assertEqual(L.snapshots()["beta"], {1: {"v": 10}, 2: {"v": 110}})
        self.assert_matches_oracle(L, plan["target"]["batch_ord"])


class TestSkipExecutionInstrumentation(Base):
    def test_skipped_batch_never_enters_interpreter(self):
        import revoke_ledger.ledger as lg
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        plan = L.plan_revocation("rev-instr", "T")
        m = mapping_by_name(plan)
        seen = []
        orig = lg.execute_program

        def counting(program, state):
            seen.append(program["logical_batch_id"])
            return orig(program, state)

        lg.execute_program = counting
        try:
            L.apply_plan(plan["plan_id"])
        finally:
            lg.execute_program = orig
        self.assertEqual(seen, [m["B"]["logical_batch_id"]])
        self.assertNotIn(m["C"]["logical_batch_id"], seen)


class TestPlanBookkeepingTamper(Base):
    def _fresh(self):
        L = new_db()
        initial, batches = main_drill()
        L.seed(initial)
        run_all(L, batches)
        return L

    def test_persisted_mapping_tamper_detected(self):
        L = self._fresh()
        plan = L.plan_revocation("rev-mt", "T")
        c = sqlite3.connect(L.db.path)
        c.execute("UPDATE revocation_mapping SET reason_code='x' "
                  "WHERE plan_id=? AND batch_ord=4", (plan["plan_id"],))
        c.commit()
        c.close()
        with self.assertRaises(StaleError) as cm:
            L.apply_plan(plan["plan_id"])
        self.assertEqual(cm.exception.code, "plan_tampered")

    def test_persisted_diff_tamper_detected(self):
        L = self._fresh()
        plan = L.plan_revocation("rev-dt", "T")
        c = sqlite3.connect(L.db.path)
        # A determined out-of-band writer can drop the trigger, as with rows.
        c.execute("DROP TRIGGER diff_no_update")
        c.execute("UPDATE revocation_diff SET new_digest='{}' "
                  "WHERE plan_id=? AND tbl='beta'", (plan["plan_id"],))
        c.commit()
        c.close()
        with self.assertRaises(StaleError) as cm:
            L.apply_plan(plan["plan_id"])
        self.assertEqual(cm.exception.code, "plan_tampered")

    def test_revocation_record_is_immutable(self):
        L = self._fresh()
        L.plan_revocation("rev-imm", "T")
        with self.assertRaises(sqlite3.IntegrityError):
            L.db.conn.execute("UPDATE revocation SET target_logical_id='B'")


class TestBatchAtomicFailure(Base):
    def test_earlier_write_rolled_back_when_later_step_fails(self):
        L = new_db()
        L.seed({"a": {1: {"v": 1}}, "b": {7: {"z": 0}}})
        gen0 = L.head()["generation"]
        r = L.submit(prog("f", [
            _read("r", "a", [1], {"kind": "all"}),
            _write("insert", "b", "r", [
                {"source_row_id": 1, "row_id": {"const": 8},
                 "values": {"z": {"const": 5}}}]),
            _write("delete", "b", "r", [
                {"source_row_id": 1, "row_id": {"const": 999}}])]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "delete_missing")
        self.assertIsNone(r["generation"])
        self.assertEqual(L.head()["generation"], gen0)
        self.assertEqual(L.snapshots()["b"], {7: {"z": 0}})
