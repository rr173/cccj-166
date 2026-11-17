"""Declarative relation integrity: tests (stdlib unittest; run from /workspace).

    python3 -m unittest discover -s tests -v

Expectations for cascade sets and final rows are computed by the independent
``constraint_oracle`` (naive repeated scans, no product imports), never by the
product's closure function.
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import constraint_oracle
from fixtures.scenarios import (CONSTRAINT_RULES, CYCLE_RULES, RESTRICT_RULES,
                                prog, _read, _write, constraint_cycle_drill,
                                constraint_delete_recreate_drill,
                                constraint_drill,
                                constraint_new_child_of_deleted_parent_drill,
                                constraint_phantom_parent_drill,
                                constraint_restrict_drill,
                                constraint_revocation_drill,
                                constraint_swap_failure_drill)
from revoke_ledger.constraints import ConstraintSet, validate_config
from revoke_ledger.errors import BatchAbort, LedgerError, ProtocolError, StaleError
from revoke_ledger.ledger import Ledger


def new_db():
    return Ledger(os.path.join(tempfile.mkdtemp(prefix="ctest-"), "c.db"))


def snap(L):
    return {t: {rid: dict(row) for rid, row in m.items()}
            for t, m in L.snapshots().items()}


def oracle_rows(initial, batches, rules, skip_ord=None):
    recs = [{"logical_batch_id": b["logical_batch_id"], "batch_ord": i + 1,
             "status": "success", "program": b} for i, b in enumerate(batches)]
    rows, outcomes, deletions = constraint_oracle.replay(
        initial, recs, rules, skip_ord or -1)
    return rows, outcomes, deletions


def seed_ledger(initial, rules):
    L = new_db()
    L.install_constraints(rules)
    L.seed(initial)
    return L


class Base(unittest.TestCase):
    def assert_rows_equal(self, actual, expected):
        # An empty table is not materialised in a generation snapshot.
        self.assertEqual({t: m for t, m in actual.items() if m},
                         {t: dict(m) for t, m in expected.items() if m})


# ---------------------------------------------------------------------------
class TestConfiguration(Base):
    def test_install_and_read_back(self):
        L = new_db()
        view = L.install_constraints(CONSTRAINT_RULES)
        self.assertTrue(view["installed"])
        self.assertEqual(view["schema"], "managed-constraints/v1")
        self.assertEqual(len(view["rules"]), len(CONSTRAINT_RULES["rules"]))
        self.assertEqual(view["installed_at_generation"], 1)
        self.assertTrue(view["installed_before_ingest"])
        self.assertIn("owner", view["participating_tables"])
        # Persisted read-back survives a reopen with identical bytes.
        L.close()
        L2 = Ledger(L.db.path)
        self.assertEqual(L2.constraints_view()["rules"],
                         view["rules"])
        self.assertEqual(L2.constraints_view()["config_hash"],
                         view["config_hash"])

    def test_invalid_config_installs_nothing(self):
        L = new_db()
        bad = {"schema": "managed-constraints/v1", "rules": [
            {"name": "a", "kind": "unique", "table": "t", "fields": ["x"]},
            {"name": "a", "kind": "unique", "table": "u", "fields": ["y"]}]}
        with self.assertRaises(ProtocolError):
            L.install_constraints(bad)
        self.assertIsNone(L.db.constraint_config())
        self.assertFalse(L.constraints_view()["installed"])
        # The ledger still works under legacy semantics.
        L.seed({"t": {1: {"x": 1}}})
        self.assertIsNone(L.constraints())

    def test_bad_on_delete_and_shapes_rejected(self):
        cases = [
            {"schema": "managed-constraints/v1", "rules": [{"name": "f",
             "kind": "foreign_key", "child": "c", "field": "p", "parent": "p",
             "on_delete": "SET NULL"}]},
            {"schema": "managed-constraints/v1", "rules": [{"name": "u",
             "kind": "unique", "table": "t", "fields": []}]},
            {"schema": "managed-constraints/v1", "rules": [{"name": "u",
             "kind": "unique", "table": "t", "fields": ["a", "a"]}]},
            {"schema": "managed-constraints/v1", "rules": [{"name": "",
             "kind": "unique", "table": "t", "fields": ["a"]}]},
            {"schema": "wrong", "rules": []},
        ]
        for cfg in cases:
            with self.subTest(cfg=cfg):
                with self.assertRaises(ProtocolError):
                    validate_config(cfg)

    def test_rules_are_immutable_after_install(self):
        L = new_db()
        L.install_constraints(CONSTRAINT_RULES)
        with self.assertRaises(LedgerError):
            L.install_constraints({"schema": "managed-constraints/v1",
                                   "rules": []})
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            L.db.conn.execute("UPDATE constraint_config SET rules_json='[]'")

    def test_must_install_before_seed_or_ingest(self):
        L = new_db()
        L.seed({"t": {1: {"x": 1}}})
        with self.assertRaises(LedgerError):
            L.install_constraints(CONSTRAINT_RULES)
        L2 = new_db()
        L2.ingest(prog("p", [_read("r", "t", [1], {"kind": "all"})]))
        with self.assertRaises(LedgerError):
            L2.install_constraints(CONSTRAINT_RULES)

    def test_seed_must_satisfy_rules(self):
        L = new_db()
        rules = {"schema": "managed-constraints/v1", "rules": [
            {"name": "u", "kind": "unique", "table": "t", "fields": ["x"]}]}
        L.install_constraints(rules)
        with self.assertRaises(BatchAbort) as cm:
            L.seed({"t": {1: {"x": 1}, 2: {"x": 1}}})
        self.assertEqual(cm.exception.code, "constraint_violation")
        self.assertEqual(L.snapshots(), {})
        self.assertEqual(L.head()["generation"], 1)

    def test_legacy_unconfigured_behaviour_unchanged(self):
        L = new_db()
        L.seed({"t": {1: {"x": 1}}})
        r = L.submit(prog("p", [_read("r", "t", [1], {"kind": "all"})]))
        self.assertEqual(r["status"], "success")
        self.assertIsNone(L.constraints())

    def test_type_sensitive_unique_keys(self):
        L = new_db()
        L.install_constraints({"schema": "managed-constraints/v1", "rules": [
            {"name": "u", "kind": "unique", "table": "t", "fields": ["x"]}]})
        # 1, true, "1" are distinct; nulls do not participate at all.
        L.seed({"t": {1: {"x": 1}, 2: {"x": True}, 3: {"x": "1"},
                      4: {"x": None}, 5: {}}, "s": {1: {"z": 0}}})
        self.assertEqual(len(L.snapshots()["t"]), 5)
        r = L.submit(prog("N", [
            _read("s", "s", [1], {"kind": "all"}),
            _write("replace", "t", "s", [
                {"source_row_id": 1, "row_id": {"const": 2},
                 "values": {"x": {"const": 1}}}])]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "constraint_violation")
        reason = r["abort"]["reasons"][0]
        self.assertEqual(reason["code"], "unique_violation")
        self.assertEqual(reason["rule"], "u")
        self.assertEqual(reason["row_ids"], [1, 2])
        self.assertEqual(reason["values"], [{"type": "int", "value": 1}])

    def test_bool_foreign_key_is_not_integer(self):
        L = new_db()
        L.install_constraints({"schema": "managed-constraints/v1", "rules": [
            {"name": "f", "kind": "foreign_key", "child": "c", "field": "p",
             "parent": "p", "on_delete": "CASCADE"}]})
        L.seed({"p": {1: {"z": 0}}, "s": {1: {"z": 0}}})
        r = L.submit(prog("B", [
            _read("s", "s", [1], {"kind": "all"}),
            _write("insert", "c", "s", [
                {"source_row_id": 1, "row_id": {"const": 5},
                 "values": {"p": {"const": True}}}])]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["reasons"][0]["code"], "fk_type")
        # Missing field means null -> no parent required.
        r = L.submit(prog("M", [
            _read("s", "s", [1], {"kind": "all"}),
            _write("insert", "c", "s", [
                {"source_row_id": 1, "row_id": {"const": 6},
                 "values": {"q": {"const": 1}}}])]))
        self.assertEqual(r["status"], "success")


# ---------------------------------------------------------------------------
class TestBatchFinalChecks(Base):
    def test_child_inserted_before_parent_succeeds(self):
        initial, batches = constraint_drill()
        L = seed_ledger(initial, CONSTRAINT_RULES)
        r = L.submit(batches[0])          # B1: entry -> account -> owner
        self.assertEqual(r["status"], "success", r["abort"])
        rows = snap(L)
        self.assertEqual(rows["entry"][200],
                         {"account_id": 20, "kind": "debit"})
        self.assertEqual(rows["account"][20], {"owner_id": 3})
        self.assertEqual(rows["owner"][3], {"code": "acme-2"})
        # Intermediate read of a transiently visible child is recorded.
        self.assertEqual(r["evidence"]["reads"][0]["selected_ids"], [1000])

    def test_unique_swap_succeeds(self):
        initial, batches = constraint_drill()
        L = seed_ledger(initial, CONSTRAINT_RULES)
        L.submit(batches[0])
        r = L.submit(batches[1])          # B2: swap owner codes
        self.assertEqual(r["status"], "success", r["abort"])
        self.assertEqual(snap(L)["owner"][1], {"code": "beta"})
        self.assertEqual(snap(L)["owner"][2], {"code": "acme"})

    def test_duplicate_after_swap_rejects_whole_batch(self):
        initial, batches = constraint_swap_failure_drill()
        L = seed_ledger(initial, CONSTRAINT_RULES)
        before_head = L.head()
        r = L.submit(batches[0])
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "constraint_violation")
        self.assertEqual(r["abort"]["reasons"][0]["code"], "unique_violation")
        self.assertEqual(r["abort"]["reasons"][0]["row_ids"], [1, 3])
        self.assertIsNone(r["generation"])
        self.assertEqual(L.head(), before_head)     # no generation committed
        self.assertEqual(snap(L)["owner"],
                         {1: {"code": "a"}, 2: {"code": "b"}, 3: {"code": "c"}})

    def test_intermediate_duplicate_inside_step_is_fine(self):
        # Part 1 writes a duplicate, part 2 fixes it: only the completed batch
        # is checked.
        L = new_db()
        L.install_constraints({"schema": "managed-constraints/v1", "rules": [
            {"name": "u", "kind": "unique", "table": "t", "fields": ["x"]}]})
        L.seed({"t": {1: {"x": "a"}, 2: {"x": "b"}}, "s": {1: {"z": 0}}})
        L.ingest(prog("S", [
            _read("s", "s", [1], {"kind": "all"}),
            _write("replace", "t", "s", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"x": {"const": "b"}}}]),
            _write("replace", "t", "s", [
                {"source_row_id": 1, "row_id": {"const": 2},
                 "values": {"x": {"const": "z"}}}]),
        ]))
        # /!\ the two replaces are in different steps but the same alias, and
        # the intermediate state is a duplicate.  The batch must still succeed.
        r = L.run_ingested("S")
        self.assertEqual(r["status"], "success", r["abort"])
        self.assertEqual(snap(L)["t"], {1: {"x": "b"}, 2: {"x": "z"}})

    def test_pk_conflict_and_guard_still_apply(self):
        initial, batches = constraint_drill()
        L = seed_ledger(initial, CONSTRAINT_RULES)
        L.submit(batches[0])
        r = L.submit(prog("K", [
            _read("s", "scratch", [1000], {"kind": "all"}),
            _write("insert", "owner", "s", [
                {"source_row_id": 1000, "row_id": {"const": 3},
                 "values": {"code": {"const": "dup-pk"}}}]),
        ]))
        self.assertEqual(r["abort"]["code"], "pk_conflict")
        r = L.submit(prog("G", [
            _read("s", "scratch", [1000], {"kind": "all"}),
            {"op": "guard", "test": "row_count", "alias": "s", "eq": 5},
        ]))
        self.assertEqual(r["abort"]["code"], "guard_failed")


# ---------------------------------------------------------------------------
class TestCascadeSemantics(Base):
    def test_cycle_and_two_paths_delete_deterministically(self):
        initial, batches = constraint_cycle_drill()
        L = seed_ledger(initial, CYCLE_RULES)
        r = L.submit(batches[0])
        self.assertEqual(r["status"], "success", r["abort"])
        report = r["evidence"]["constraints"]
        product_set = sorted([d["table"], d["row_id"]] for d in report["deleted"])

        # Independent expectation: full replay through the product-free
        # interpreter and the naive repeated-scan closure inside it.
        _, _, deletions = oracle_rows(initial, [batches[0]],
                                      CYCLE_RULES["rules"])
        expected = deletions[1]
        self.assertEqual(product_set, sorted([list(k) for k in expected]))
        self.assertEqual(product_set, [["node", 1], ["node", 2], ["node", 3],
                                       ["node", 7], ["node", 8], ["node", 9],
                                       ["owner", 1]])
        # node[9] is reached twice (owner FK and parent chain); it is deleted
        # once and both parents are recorded.
        node9 = [d for d in report["deleted"]
                 if d["table"] == "node" and d["row_id"] == 9][0]
        self.assertTrue(node9["implicit"])
        self.assertEqual(node9["extra_parents"], [["node", 2]])
        # Explicit vs implicit are distinguished.
        explicit = [d for d in report["deleted"] if d["explicit"]]
        self.assertEqual(explicit, [dict(
            d, trigger_chain=[], root=["owner", 1]) for d in explicit])
        self.assertEqual([(d["table"], d["row_id"]) for d in explicit],
                         [("owner", 1)])
        self.assertEqual(report["implicit_deletes"],
                         [["node", 1], ["node", 2], ["node", 3],
                          ["node", 7], ["node", 8], ["node", 9]])
        # A trigger chain is traceable root -> ... -> victim.
        node3 = [d for d in report["deleted"] if d["table"] == "node"
                 and d["row_id"] == 3][0]
        self.assertEqual([(h["from_table"], h["from_row_id"],
                           h["to_table"], h["to_row_id"])
                          for h in node3["trigger_chain"]],
                         [("owner", 1, "node", 1),
                          ("node", 1, "node", 2),
                          ("node", 2, "node", 3)])
        self.assertEqual(snap(L), {"owner": {2: {"code": "keep"}},
                                   "scratch": {1: {"z": 0}}})

    def test_cycle_revocation_matches_full_replay(self):
        initial, batches = constraint_cycle_drill()
        L = seed_ledger(initial, CYCLE_RULES)
        for b in batches:
            L.submit(b)
        plan = L.plan_revocation("rv-cycle", "T")
        res = L.apply_plan(plan["plan_id"])
        expected, _, _ = oracle_rows(initial, batches, CYCLE_RULES["rules"],
                                     skip_ord=1)
        self.assert_rows_equal(snap(L), expected)
        self.assertEqual(res["blocked_batch_ords"], [])

    def test_restrict_child_surviving_rejects_batch(self):
        initial, batches = constraint_restrict_drill(False)
        L = seed_ledger(initial, RESTRICT_RULES)
        before = snap(L)
        r = L.submit(batches[0])
        self.assertEqual(r["status"], "failed")
        reasons = r["abort"]["reasons"]
        self.assertEqual(reasons[0]["code"], "restrict_violation")
        self.assertEqual(reasons[0]["rule"], "note_owner_fk")
        self.assertEqual(reasons[0]["table"], "note")
        self.assertEqual(reasons[0]["row_id"], 900)
        self.assertEqual(reasons[0]["deleted_parent"], ["owner", 1])
        self.assertEqual(snap(L), before)      # old data untouched

    def test_restrict_child_swept_by_another_cascade_allows_delete(self):
        initial, batches = constraint_restrict_drill(True)
        L = seed_ledger(initial, RESTRICT_RULES)
        r = L.submit(batches[0])
        self.assertEqual(r["status"], "success", r["abort"])
        rep = r["evidence"]["constraints"]
        self.assertEqual(sorted([d["table"], d["row_id"]] for d in rep["deleted"]),
                         [["account", 10], ["ledger", 50], ["note", 900],
                          ["owner", 1]])
        note = [d for d in rep["deleted"] if d["table"] == "note"][0]
        self.assertEqual(note["rule"], "note_ledger_fk")
        self.assertEqual(note["trigger_chain"][0]["rule"], "note_ledger_fk")
        self.assertEqual(snap(L), {"scratch": {1: {"z": 0}}})

    def test_new_child_of_deleted_parent_is_cascaded(self):
        initial, batches = constraint_new_child_of_deleted_parent_drill()
        L = seed_ledger(initial, RESTRICT_RULES)
        r = L.submit(batches[0])
        self.assertEqual(r["status"], "success", r["abort"])
        rep = r["evidence"]["constraints"]
        acct = [d for d in rep["deleted"] if d["table"] == "account"][0]
        self.assertTrue(acct["implicit"])
        self.assertTrue(acct["inserted_in_batch"])
        self.assertEqual(snap(L), {"owner": {2: {"code": "b"}},
                                   "scratch": {1: {"z": 0}}})

    def test_deleted_then_recreated_parent_is_not_a_root(self):
        initial, batches = constraint_delete_recreate_drill()
        L = seed_ledger(initial, CYCLE_RULES)
        r = L.submit(batches[0])
        self.assertEqual(r["status"], "success", r["abort"])
        self.assertEqual(r["evidence"]["constraints"]["roots"], [])
        self.assertEqual(r["evidence"]["constraints"]["deleted"], [])
        self.assertEqual(snap(L)["account"], {10: {"owner_id": 1}})
        self.assertEqual(snap(L)["owner"], {1: {"code": "a"}})

    def test_phantom_parent_is_rejected_not_cascaded(self):
        initial, batches = constraint_phantom_parent_drill()
        L = seed_ledger(initial, RESTRICT_RULES)
        r = L.submit(batches[0])
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "constraint_violation")
        reason = r["abort"]["reasons"][0]
        self.assertEqual(reason["code"], "fk_missing")
        self.assertEqual(reason["referenced_row_id"], 404)
        self.assertEqual(reason["table"], "account")
        self.assertEqual(reason["row_id"], 55)
        self.assertIsNone(L.snapshots().get("account", {}).get(55))


# ---------------------------------------------------------------------------
class TestConstraintRevocation(Base):
    def test_later_cross_table_child_blocked_after_revoking_parent_batch(self):
        initial, batches = constraint_revocation_drill()
        L = seed_ledger(initial, CONSTRAINT_RULES)
        for b in batches:
            r = L.submit(b)
            self.assertEqual(r["status"], "success")
        plan = L.plan_revocation("rv-parent", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertEqual(m["T"]["disposition"], "Superseded")
        # L declares a partition on its own read/write table `account` and
        # never reads `owner`; the declared FK still makes it dependent.
        self.assertEqual(m["L"]["disposition"], "Blocked")
        self.assertEqual(m["L"]["reason_code"], "blocked_constraint_violation")
        self.assertEqual(m["L"]["detail"]["constraint_reasons"][0]["code"],
                         "fk_missing")
        self.assertEqual(m["L"]["detail"]["constraint_reasons"][0]
                         ["referenced_row_id"], 5)
        # The truly unrelated note batch is still skipped.
        self.assertEqual(m["Z"]["reuse_kind"], "skipped_independent")

        res = L.apply_plan(plan["plan_id"])
        self.assertIn(m["L"]["batch_ord"], res["blocked_batch_ords"])
        self.assertNotIn(m["L"]["batch_ord"], res["replayed_batch_ords"])
        self.assertIn(m["Z"]["batch_ord"], res["skipped_independent_batch_ords"])
        self.assertNotIn("L", [b["logical_batch_id"] for b in
                               L.db.batches() if b["status"] == "success"
                               and b["generation"] == res["new_generation"]])
        self.assertNotIn(2, snap(L)["account"])
        self.assertIn(7, snap(L)["note"])

        # Cross-check final state & outcomes with the independent replay.
        expected, outcomes, _ = oracle_rows(initial, batches,
                                            CONSTRAINT_RULES["rules"],
                                            skip_ord=1)
        self.assert_rows_equal(snap(L), expected)
        self.assertEqual(outcomes[2]["status"], "Blocked")
        self.assertEqual(outcomes[2]["code"], "constraint_violation")

        # Reopening the database gives the same committed result and reasons.
        gen = L.head()["generation"]
        L.close()
        L2 = Ledger(L.db.path)
        self.assertEqual(L2.head()["generation"], gen)
        expl = L2.explain_revocation("rv-parent")
        self.assertFalse(expl["batches"]["L"]["skipped"])
        self.assertEqual(expl["blocked"], {"L": "blocked_constraint_violation"})
        self.assertEqual(expl["batches"]["L"]["constraint_reasons"][0]["rule"],
                         "account_owner_fk")
        trace = L2.db.generation_info(gen)["trace"]["events"][0]
        self.assertIn(m["L"]["batch_ord"], trace["blocked_batch_ords"])
        self.assertIn(m["Z"]["batch_ord"], trace["skipped_batch_ords"])

    def test_no_partial_cascade_survives_a_blocked_replay(self):
        """A replay whose completed state is invalid must not keep its cascade."""
        # T inserts owner[1]; L deletes owner[2] (cascading account[2] and
        # note[900]) while inserting note[901] referencing the *deleted*
        # owner[2] under a CASCADE-only rule -> after cascade note[901] would
        # be fine only if the delete were kept.  The real trap: insert a note
        # that references a parent that never exists, so the batch is blocked;
        # its earlier cascade must be rolled back.
        initial = {
            "owner": {2: {"code": "b"}},
            "account": {2: {"owner_id": 2}},
            "note": {900: {"owner_id": 2}},
            "scratch": {1: {"z": 0}},
        }
        rules = {"schema": "managed-constraints/v1", "rules": [
            {"name": "acct_fk", "kind": "foreign_key", "child": "account",
             "field": "owner_id", "parent": "owner", "on_delete": "CASCADE"},
            {"name": "note_fk", "kind": "foreign_key", "child": "note",
             "field": "owner_id", "parent": "owner", "on_delete": "CASCADE"}]}
        L = seed_ledger(initial, rules)
        before = snap(L)
        r = L.submit(prog("BAD", [
            _read("s", "scratch", [1], {"kind": "all"}),
            _write("delete", "owner", "s", [
                {"source_row_id": 1, "row_id": {"const": 2}}]),
            _write("insert", "note", "s", [
                {"source_row_id": 1, "row_id": {"const": 901},
                 "values": {"owner_id": {"const": 777}}}]),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["reasons"][0]["code"], "fk_missing")
        self.assertEqual(snap(L), before)
        self.assertIn(2, snap(L)["owner"])
        self.assertIn(2, snap(L)["account"])
        self.assertIn(900, snap(L)["note"])

    def test_constraint_reasons_survive_revocation_without_reverse_cascade(self):
        """Revocation result is re-derived, not an inverse cascade log."""
        initial, batches = constraint_drill()
        L = seed_ledger(initial, CONSTRAINT_RULES)
        for b in batches[:2]:
            L.submit(b)
        pre = snap(L)
        plan = L.plan_revocation("rv-b2", "B2")
        L.ingest(prog("Q", [_read("s", "scratch", [1003], {"kind": "all"}),
                            _write("replace", "scratch", "s", [
                                {"source_row_id": 1003,
                                 "row_id": {"const": 1003},
                                 "values": {"z": {"const": 1}}}])]))
        with self.assertRaises(StaleError):
            L.apply_plan(plan["plan_id"])
        self.assertEqual(snap(L), pre)

    def test_skip_proof_uses_constraint_dependencies(self):
        """A batch partitioned on its own table is demoted when a referenced
        parent actually changes, even though its explicit tables are disjoint."""
        initial, batches = constraint_revocation_drill()
        L = seed_ledger(initial, CONSTRAINT_RULES)
        for b in batches:
            L.submit(b)
        plan = L.plan_revocation("rv-dep", "T")
        el = plan["skip_eligibility"]
        # L's structural partition is eligible; the proof then demotes it.
        self.assertTrue(el["2"]["eligible"])
        self.assertTrue(any("constraint" in w or "re-check" in w
                            for w in el["2"]["why"]))
        # Z is unaffected and its dependency closure is recorded explicitly.
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertEqual(m["Z"]["detail"]["actual_dependency_overlap"], [])
        self.assertIn(["owner", 9],
                      m["Z"]["detail"]["constraint_dependency_keys"])

    def test_true_partition_skip_preserved_with_constraints(self):
        """Genuinely unrelated partition still skips (not everything replays)."""
        L = new_db()
        L.install_constraints({"schema": "managed-constraints/v1", "rules": [
            {"name": "u", "kind": "unique", "table": "alpha",
             "fields": ["amount"]},
            {"name": "f", "kind": "foreign_key", "child": "beta",
             "field": "owner_id", "parent": "alpha", "on_delete": "CASCADE"},
            {"name": "g", "kind": "foreign_key", "child": "gamma",
             "field": "alpha_id", "parent": "alpha", "on_delete": "CASCADE"}]})
        L.seed({"alpha": {1: {"amount": 10}, 2: {"amount": 4}},
                "beta": {}, "gamma": {900: {"alpha_id": 2}}})
        T = prog("T", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"const": 20}}}])])
        C = prog("C", [
            _read("g", "gamma", [900], {"kind": "all"}),
            _write("replace", "gamma", "g", [
                {"source_row_id": 900, "row_id": {"const": 900},
                 "values": {"alpha_id": {"const": 2}}}])],
            partition={"table": "gamma", "ranges": [[900, 900]]})
        L.submit(T)
        L.submit(C)
        plan = L.plan_revocation("rv-skip", "T")
        res = L.apply_plan(plan["plan_id"])
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertEqual(m["C"]["reuse_kind"], "skipped_independent")
        self.assertIn(m["C"]["batch_ord"], res["skipped_independent_batch_ords"])
        self.assertNotIn(m["C"]["batch_ord"], res["replayed_batch_ords"])
        self.assertEqual(snap(L)["gamma"][900], {"alpha_id": 2})


class TestPersistedExplanation(Base):
    def test_batch_components_are_readable_and_reopen_stable(self):
        initial, batches = constraint_cycle_drill()
        L = seed_ledger(initial, CYCLE_RULES)
        r = L.submit(batches[0])
        expl = L.explain_batch("T")
        self.assertEqual(expl["row_effects"]["explicit"], [["owner", 1]])
        self.assertEqual(expl["row_effects"]["implicit"],
                         [["node", 1], ["node", 2], ["node", 3],
                          ["node", 7], ["node", 8], ["node", 9]])
        self.assertEqual(expl["constraint_config"]["rule_count"], 2)
        self.assertEqual(expl["abort_code"], None)
        trace = L.db.generation_info(r["generation"])["trace"]["events"][0]
        comp = trace["components"]
        self.assertEqual(comp["row_writes"]["implicit"],
                         [["node", 1], ["node", 2], ["node", 3],
                          ["node", 7], ["node", 8], ["node", 9]])
        self.assertEqual(comp["constraint_checks"]["ok"], True)
        self.assertEqual(sorted(comp["rules_triggered"]),
                         ["node_owner_fk", "node_parent_fk"])
        L.close()
        L2 = Ledger(L.db.path)
        self.assertEqual(L2.explain_batch("T")["row_effects"]["implicit"],
                         expl["row_effects"]["implicit"])

    def test_blocked_batch_records_reasons_and_no_generation(self):
        initial, batches = constraint_phantom_parent_drill()
        L = seed_ledger(initial, RESTRICT_RULES)
        head = L.head()
        r = L.submit(batches[0])
        self.assertEqual(r["status"], "failed")
        self.assertIsNone(r["generation"])
        self.assertEqual(L.head(), head)
        expl = L.explain_batch("T")
        self.assertEqual(expl["abort_code"], "constraint_violation")
        self.assertEqual(expl["constraint_reasons"][0]["code"], "fk_missing")
        self.assertEqual(expl["row_effects"]["implicit"], [])


if __name__ == "__main__":
    unittest.main()


class TestUniqueOccupancySkipProof(Base):
    def test_partition_replayed_when_unique_group_changes(self):
        """Unique occupancy on another row is a constraint dependency."""
        rules = {"schema": "managed-constraints/v1", "rules": [
            {"name": "owner_code_u", "kind": "unique", "table": "owner",
             "fields": ["code"]},
            {"name": "acct_owner", "kind": "foreign_key", "child": "account",
             "field": "owner_id", "parent": "owner", "on_delete": "CASCADE"}]}
        initial = {"owner": {1: {"code": "a"}, 2: {"code": "b"},
                             3: {"code": "c"}},
                   "account": {20: {"owner_id": 2}},
                   "scratch": {1: {"z": 0}}}
        T = prog("T", [_read("s", "scratch", [1], {"kind": "all"}),
                       _write("delete", "owner", "s", [
                           {"source_row_id": 1, "row_id": {"const": 2}}])])
        # D declares a partition on owner[3] only -- structurally disjoint from
        # T's write keys and read scope -- but it sets owner[3].code='b', which
        # was free only because T had removed owner[2].  The unique group makes
        # it dependent, so it must be replayed (and then blocked).
        D = prog("D", [_read("o", "owner", [3], {"kind": "all"}),
                       _write("replace", "owner", "o", [
                           {"source_row_id": 3, "row_id": {"const": 3},
                            "values": {"code": {"const": "b"}}}])],
                 partition={"table": "owner", "ranges": [[3, 3]]})
        L = seed_ledger(initial, rules)
        self.assertEqual(L.submit(T)["status"], "success")
        self.assertEqual(L.submit(D)["status"], "success")
        plan = L.plan_revocation("rv-u", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertTrue(plan["skip_eligibility"][str(m["D"]["batch_ord"])]
                        ["eligible"])
        self.assertEqual(m["D"]["disposition"], "Blocked")
        self.assertEqual(m["D"]["reason_code"], "blocked_constraint_violation")
        res = L.apply_plan(plan["plan_id"])
        self.assertNotIn(m["D"]["batch_ord"], res["skipped_independent_batch_ords"])
        self.assertEqual(snap(L)["owner"],
                         {1: {"code": "a"}, 2: {"code": "b"}, 3: {"code": "c"}})


class TestConstraintAwareCascadeReuse(Base):
    def test_skipped_batch_cascade_outputs_proven_and_reused(self):
        """A batch with recorded cascade deletes may skip only if the derived
        state reproduces exactly the same implicit deletion set."""
        rules = {"schema": "managed-constraints/v1", "rules": [
            {"name": "acct_fk", "kind": "foreign_key", "child": "account",
             "field": "owner_id", "parent": "owner", "on_delete": "CASCADE"},
            {"name": "gamma_fk", "kind": "foreign_key", "child": "gamma",
             "field": "account_id", "parent": "account", "on_delete": "CASCADE"}]}
        initial = {"owner": {1: {"code": "keep"}, 2: {"code": "gone"}},
                   "account": {20: {"owner_id": 2}, 21: {"owner_id": 1}},
                   "gamma": {900: {"account_id": 20}, 901: {"account_id": 21}},
                   "scratch": {1: {"z": 0}}}
        L = seed_ledger(initial, rules)
        # T touches only scratch/owner[1] value space, not owner[2].
        T = prog("T", [_read("s", "scratch", [1], {"kind": "all"}),
                       _write("replace", "owner", "s", [
                           {"source_row_id": 1, "row_id": {"const": 1},
                            "values": {"code": {"const": "keep"}}}])])
        # D deletes owner[2]; its cascade sweeps account[20] and gamma[900].
        D = prog("D", [_read("s", "scratch", [1], {"kind": "all"}),
                       _write("delete", "owner", "s", [
                           {"source_row_id": 1, "row_id": {"const": 2}}])],
                 partition={"table": "owner", "ranges": [[2, 2]]})
        L.submit(T)
        r = L.submit(D)
        self.assertEqual(r["status"], "success")
        self.assertEqual(r["evidence"]["constraints"]["implicit_deletes"],
                         [["account", 20], ["gamma", 900]])
        plan = L.plan_revocation("rv-c", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        # D's recorded cascade leaves its declared partition -> conservative replay.
        self.assertEqual(m["D"]["disposition"], "Kept")
        self.assertFalse(plan["skip_eligibility"][str(m["D"]["batch_ord"])]
                         ["eligible"])
        res = L.apply_plan(plan["plan_id"])
        # D was replayed (not skipped) from the pre-target state, so its
        # recorded cascade is re-derived there: owner[2] and its children go.
        self.assertEqual(snap(L)["gamma"], {901: {"account_id": 21}})
        self.assertEqual(snap(L)["owner"], {1: {"code": "keep"}})
        self.assertEqual(snap(L).get("account", {}), {21: {"owner_id": 1}})

    def test_constraint_only_batch_still_skips(self):
        """A batch whose writes stay inside its partition and cascade nowhere
        is genuinely independent and is skipped, with dependency keys recorded."""
        rules = {"schema": "managed-constraints/v1", "rules": [
            {"name": "acct_fk", "kind": "foreign_key", "child": "account",
             "field": "owner_id", "parent": "owner", "on_delete": "CASCADE"}]}
        initial = {"owner": {1: {"code": "a"}}, "account": {},
                   "scratch": {1: {"z": 0}},
                   "gamma": {500: {"v": 1}}}
        T = prog("T", [_read("s", "scratch", [1], {"kind": "all"}),
                       _write("insert", "account", "s", [
                           {"source_row_id": 1, "row_id": {"const": 30},
                            "values": {"owner_id": {"const": 1}}}])])
        C = prog("C", [_read("g", "gamma", [500], {"kind": "all"}),
                       _write("replace", "gamma", "g", [
                           {"source_row_id": 500, "row_id": {"const": 500},
                            "values": {"v": {"const": 2}}}])],
                 partition={"table": "gamma", "ranges": [[500, 500]]})
        L = seed_ledger(initial, rules)
        L.submit(T)
        L.submit(C)
        plan = L.plan_revocation("rv-i", "T")
        res = L.apply_plan(plan["plan_id"])
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertEqual(m["C"]["reuse_kind"], "skipped_independent")
        self.assertIn(m["C"]["batch_ord"], res["skipped_independent_batch_ords"])
        # The skip proof records the constraint closure it examined (empty of
        # changed keys here).
        self.assertEqual(m["C"]["detail"]["actual_dependency_overlap"], [])
        self.assertEqual(snap(L).get("account", {}), {})
        self.assertEqual(snap(L)["gamma"], {500: {"v": 2}})
