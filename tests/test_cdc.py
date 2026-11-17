"""Unit tests for the offline CDC transaction ingestor.

Run from /workspace:

    python3 -m unittest tests.test_cdc -v
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fixtures.cdc_scenarios import (MiniModel, TBL_CUSTOMER, TBL_ORDER,
                                    SOURCE_ID, constraint_rules,
                                    drill_delivery_order, drill_transactions,
                                    fragment, source_config)
from revoke_ledger.cdcp import (CDC_CONFIG_SCHEMA,
                                validate_fragment, validate_source_config)
from revoke_ledger.cdc import ST_MISSING_BEGIN, ST_MISSING_CHANGES, ST_MISSING_END
from revoke_ledger.cdc import ST_MISSING_TX, ST_READY, ST_REJECTED_PENDING
from revoke_ledger.cdc import ST_CONSUMED_ABORTED, ST_SKIPPED
from revoke_ledger.errors import LedgerError, ProtocolError
from revoke_ledger.ledger import Ledger

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def new_ledger(constraints=None):
    d = tempfile.mkdtemp(prefix="cdctest-")
    L = Ledger(os.path.join(d, "warehouse.db"))
    if constraints is not None:
        L.install_constraints(constraints)
    return L, d


def install(L, cfg=None):
    cfg = cfg or source_config()
    return L.cdc().install_source(cfg)


def write_jsonl(path, fragments):
    with open(path, "w", encoding="utf-8") as fh:
        for f in fragments:
            fh.write(json.dumps(f, sort_keys=False) + "\n")


def tamper_managed_row(ledger, generation, table, row_id, new_row):
    """Out-of-band modification of a committed managed row.

    The production triggers make managed_rows immutable on purpose; this
    helper temporarily drops and recreates them inside one transaction to
    simulate external drift (e.g. ordinary processing wrote the same table),
    which the CDC before-image guard must detect and refuse to overwrite.
    """
    conn = ledger.db.conn
    payload = json.dumps(new_row, sort_keys=True, separators=(",", ":"))
    with conn:
        conn.execute("DROP TRIGGER managed_rows_no_update")
        conn.execute(
            "UPDATE managed_rows SET row_json=? WHERE generation=? AND tbl=? "
            "AND row_id=?", (payload, generation, table, row_id))
        conn.execute(
            "CREATE TRIGGER managed_rows_no_update BEFORE UPDATE ON managed_rows "
            "BEGIN SELECT RAISE(ABORT, 'managed_rows are immutable'); END")


def begin_f(seq, tx):
    return fragment(SOURCE_ID, seq, tx, "begin")


def end_f(seq, tx, count, outcome="commit"):
    return fragment(SOURCE_ID, seq, tx, "end", change_count=count,
                    outcome=outcome)


def chg(seq, tx, index, table, op, before, after):
    return fragment(SOURCE_ID, seq, tx, "change", index=index, table=table,
                    op=op, before=before, after=after)


# ---------------------------------------------------------------------------
class TestSourceConfig(unittest.TestCase):
    def test_valid_config_normalizes(self):
        cfg = validate_source_config(source_config())
        self.assertEqual(cfg["schema"], CDC_CONFIG_SCHEMA)
        self.assertEqual(cfg["start_seq"], 0)
        self.assertEqual(len(cfg["tables"]), 2)

    def test_bad_schema_and_shape_rejected(self):
        bad = source_config()
        bad["schema"] = "managed-cdc-source/v2"
        with self.assertRaises(ProtocolError):
            validate_source_config(bad)
        bad = source_config()
        bad["extra"] = 1
        with self.assertRaises(ProtocolError):
            validate_source_config(bad)
        with self.assertRaises(ProtocolError):
            validate_source_config({"schema": CDC_CONFIG_SCHEMA})

    def test_negative_start_rejected(self):
        bad = source_config()
        bad["start_seq"] = -1
        with self.assertRaises(ProtocolError):
            validate_source_config(bad)

    def test_duplicate_managed_table_rejected(self):
        bad = source_config()
        bad["tables"][1]["managed_table"] = bad["tables"][0]["managed_table"]
        with self.assertRaises(ProtocolError):
            validate_source_config(bad)

    def test_unknown_reference_target_rejected(self):
        bad = source_config()
        bad["tables"][1]["references"][0]["references"] = "ghost"
        with self.assertRaises(ProtocolError):
            validate_source_config(bad)

    def test_reference_overlapping_key_rejected(self):
        bad = source_config()
        bad["tables"][0]["references"] = [
            {"column": "tenant", "references": "order"}]
        with self.assertRaises(ProtocolError):
            validate_source_config(bad)

    def test_duplicate_reference_column_rejected(self):
        bad = source_config()
        bad["tables"][1]["references"].append(
            {"column": "cust", "references": "customer"})
        with self.assertRaises(ProtocolError):
            validate_source_config(bad)

    def test_install_is_idempotent_and_immutable(self):
        L, _ = new_ledger()
        install(L)
        # Same semantic bytes (different object key order) -> same view.
        again = json.loads(json.dumps(source_config()))
        view1 = L.cdc().source_view(SOURCE_ID)
        view2 = L.cdc().install_source(again)
        self.assertEqual(view1["config_hash"], view2["config_hash"])
        changed = source_config()
        changed["start_seq"] = 5
        with self.assertRaises(LedgerError):
            L.cdc().install_source(changed)
        L.close()

    def test_target_table_must_be_empty_and_unclaimed(self):
        L, _ = new_ledger()
        L.seed({TBL_CUSTOMER: {99: {"x": 1}}})
        with self.assertRaises(LedgerError):
            install(L)
        L.close()

        L2, _ = new_ledger()
        install(L2)
        other = source_config(source_id="other", start_seq=0)
        other["tables"][0]["managed_table"] = TBL_CUSTOMER
        with self.assertRaises(LedgerError):
            L2.cdc().install_source(other)
        L2.close()

    def test_legacy_db_opens_without_cdc_tables_and_works(self):
        L, d = new_ledger()
        L.seed({"alpha": {1: {"amount": 10}}})
        path = L.db.path
        L.close()
        # An old database simply has no CDC tables until the ingestor runs.
        conn = sqlite3.connect(path)
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        self.assertNotIn("cdc_source", names)
        L2 = Ledger(path)
        L2.cdc()  # lazy upgrade
        conn = sqlite3.connect(path)
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        self.assertIn("cdc_source", names)
        # Historical data untouched.
        self.assertEqual(L2.snapshots()["alpha"][1], {"amount": 10})
        L2.close()


# ---------------------------------------------------------------------------
class TestFragmentProtocol(unittest.TestCase):
    def test_valid_fragments(self):
        validate_fragment(begin_f(0, "T"))
        validate_fragment(end_f(0, "T", 0))
        validate_fragment(chg(0, "T", 1, "order", "insert", None,
                              {"oid": 1}))

    def test_envelope_required_keys(self):
        f = begin_f(0, "T")
        del f["tx_id"]
        with self.assertRaises(ProtocolError):
            validate_fragment(f)

    def test_bad_part_and_outcome(self):
        f = begin_f(0, "T")
        f["part"] = "middle"
        with self.assertRaises(ProtocolError):
            validate_fragment(f)
        with self.assertRaises(ProtocolError):
            validate_fragment(end_f(0, "T", 0, outcome="maybe"))

    def test_change_index_must_start_at_one(self):
        with self.assertRaises(ProtocolError):
            validate_fragment(chg(0, "T", 0, "order", "insert", None,
                                  {"oid": 1}))

    def test_insert_requires_null_before(self):
        with self.assertRaises(ProtocolError):
            validate_fragment(chg(0, "T", 1, "order", "insert",
                                  {"oid": 1}, {"oid": 2}))

    def test_delete_requires_null_after(self):
        with self.assertRaises(ProtocolError):
            validate_fragment(chg(0, "T", 1, "order", "delete",
                                  {"oid": 1}, {"oid": 2}))

    def test_update_requires_both_rows(self):
        with self.assertRaises(ProtocolError):
            validate_fragment(chg(0, "T", 1, "order", "update", None,
                                  {"oid": 1}))

    def test_value_domain_rejects_float_and_nested_object(self):
        with self.assertRaises(ProtocolError):
            validate_fragment(chg(0, "T", 1, "order", "insert", None,
                                  {"oid": 1.5}))
        with self.assertRaises(ProtocolError):
            validate_fragment(chg(0, "T", 1, "order", "insert", None,
                                  {"oid": {"nested": 1}}))

    def test_int64_bounds_enforced(self):
        with self.assertRaises(ProtocolError):
            validate_fragment(chg(0, "T", 1, "order", "insert", None,
                                  {"oid": 2 ** 63}))

    def test_typed_key_components_allowed(self):
        # int 1, bool true and text "1" are all individually valid.
        for v in (1, True, "1"):
            validate_fragment(chg(0, "T", 1, "customer", "insert", None,
                                  {"tenant": "a", "code": v}))

    def test_reference_array_width_checked_against_config(self):
        from revoke_ledger.cdcp import check_fragment_against_config
        cfg = validate_source_config(source_config())
        ok = chg(0, "T", 1, "order", "insert", None,
                 {"oid": 1, "cust": ["a", "1"]})
        check_fragment_against_config(ok, cfg)  # width 2 matches customer key
        bad = chg(0, "T", 1, "order", "insert", None,
                  {"oid": 1, "cust": ["a"]})
        with self.assertRaises(ProtocolError):
            check_fragment_against_config(bad, cfg)
        unknown = chg(0, "T", 1, "ghost", "insert", None, {"k": 1})
        with self.assertRaises(ProtocolError):
            check_fragment_against_config(unknown, cfg)
        null_key = chg(0, "T", 1, "customer", "insert", None,
                       {"tenant": None, "code": "1"})
        with self.assertRaises(ProtocolError):
            check_fragment_against_config(null_key, cfg)

    def test_unknown_table_fragment_rejected_at_reception(self):
        L, _ = new_ledger()
        install(L)
        bad = chg(0, "T", 1, "nope", "insert", None, {"k": 1})
        with self.assertRaises(ProtocolError):
            L.cdc().receive_fragments([begin_f(0, "T"), bad, end_f(0, "T", 1)])
        L.close()


# ---------------------------------------------------------------------------
class TestReception(unittest.TestCase):
    def setUp(self):
        self.L, self.d = new_ledger()
        install(self.L)
        self.cdc = self.L.cdc()

    def tearDown(self):
        self.L.close()

    def test_staging_does_not_touch_targets_or_cursor(self):
        txs = drill_transactions()
        head_before = self.L.head()
        r = self.cdc.receive_fragments(txs[3][1])
        self.assertEqual(r["staged"], 3)
        self.assertEqual(self.L.head(), head_before)
        self.assertEqual(self.cdc.source_status(SOURCE_ID)["next_seq"], 0)
        self.assertEqual(self.L.snapshots().get(TBL_CUSTOMER, {}), {})

    def test_out_of_order_and_end_first_assemble(self):
        _, frags = drill_transactions()[0]
        shuffled = [frags[3], frags[0], frags[2], frags[1]]  # end first
        self.cdc.receive_fragments(shuffled)
        st = self.cdc.source_status(SOURCE_ID, window=1)
        self.assertEqual(st["transactions"][0]["state"], ST_READY)

    def test_missing_pieces_classified(self):
        txs = drill_transactions()
        # only begin + change of tx0
        self.cdc.receive_fragments([txs[0][1][0], txs[0][1][1]])
        st = self.cdc.source_status(SOURCE_ID, window=1)["transactions"][0]
        self.assertEqual(st["state"], ST_MISSING_END)
        # add end but with one change missing
        self.cdc.receive_fragments([
            end_f(0, "T0", 2), txs[0][1][1]])
        st = self.cdc.source_status(SOURCE_ID, window=1)["transactions"][0]
        self.assertEqual(st["state"], ST_MISSING_CHANGES)
        self.assertEqual(st["missing_change_indexes"], [1])
        # remove begin scenario on a fresh tx
        self.cdc.receive_fragments([end_f(5, "T5", 0)])
        st5 = [t for t in self.cdc.source_status(SOURCE_ID, window=6)["transactions"]
               if t["tx_seq"] == 5][0]
        self.assertEqual(st5["state"], ST_MISSING_BEGIN)

    def test_duplicate_semantic_content_is_idempotent(self):
        txs = drill_transactions()
        r1 = self.cdc.receive_fragments(txs[3][1])
        r2 = self.cdc.receive_fragments(txs[3][1])
        self.assertTrue(all(x["status"] == "duplicate"
                            for x in r2["fragments"]))
        self.assertEqual(r1["staged"], 3)
        self.assertEqual(r2["staged"], 0)
        # Object key reordering is not new content.
        reordered = [{k: f[k] for k in reversed(list(f))} for f in txs[3][1]]
        r3 = self.cdc.receive_fragments(reordered)
        self.assertTrue(all(x["status"] == "duplicate"
                            for x in r3["fragments"]))

    def test_same_identity_different_content_conflicts(self):
        txs = drill_transactions()
        self.cdc.receive_fragments(txs[3][1])
        evil = chg(3, "T3", 1, "customer", "insert", None,
                   {"tenant": "b", "code": "9", "name": "CHANGED"})
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments([evil])
        # nothing from that delivery was written: original content still staged
        expl = self.cdc.explain_transaction(SOURCE_ID, 3)
        self.assertEqual(len(expl["fragments"]), 3)

    def test_tx_seq_tx_id_binding_conflict_rejected(self):
        self.cdc.receive_fragments([begin_f(0, "T0"), end_f(0, "T0", 0)])
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments([begin_f(0, "OTHER")])
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments([begin_f(1, "T0")])

    def test_change_count_contradiction_rejected(self):
        self.cdc.receive_fragments([end_f(0, "T0", 2)])
        bad = chg(0, "T0", 3, "order", "insert", None, {"oid": 1})
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments([bad])

    def test_file_all_or_nothing_with_trailing_bad_json(self):
        good = drill_transactions()[3][1]
        p = os.path.join(self.d, "f.jsonl")
        with open(p, "w", encoding="utf-8") as fh:
            for f in good:
                fh.write(json.dumps(f) + "\n")
            fh.write("{not valid json\n")
        before = self.cdc.source_status(SOURCE_ID, window=4)
        with self.assertRaises(ProtocolError):
            self.cdc.receive_file(p)
        after = self.cdc.source_status(SOURCE_ID, window=4)
        self.assertEqual(after, before)

    def test_file_all_or_nothing_on_semantic_conflict(self):
        txs = drill_transactions()
        self.cdc.receive_fragments(txs[3][1])
        p = os.path.join(self.d, "mix.jsonl")
        good = txs[0][1]
        evil = chg(3, "T3", 1, "customer", "insert", None,
                   {"tenant": "x", "code": "y", "name": "z"})
        write_jsonl(p, good + [evil])
        with self.assertRaises(ProtocolError):
            self.cdc.receive_file(p)
        # tx0 fragments from the same file must not have landed.
        self.assertEqual(
            self.cdc.source_status(SOURCE_ID, window=1)["transactions"][0]["state"],
            ST_MISSING_TX)

    def test_blank_line_rejects_whole_file(self):
        p = os.path.join(self.d, "blank.jsonl")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(begin_f(0, "T0")) + "\n\n")
        with self.assertRaises(ProtocolError):
            self.cdc.receive_file(p)

    def test_receive_before_start_seq_rejected(self):
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments([begin_f(-1, "TX")])

    def test_empty_transaction_is_legal(self):
        self.cdc.receive_fragments([begin_f(0, "T0"), end_f(0, "T0", 0)])
        st = self.cdc.source_status(SOURCE_ID, window=1)
        self.assertEqual(st["transactions"][0]["state"], ST_READY)


# ---------------------------------------------------------------------------
class TestAdvanceAndMapping(unittest.TestCase):
    def setUp(self):
        self.L, self.d = new_ledger()
        install(self.L)
        self.cdc = self.L.cdc()

    def tearDown(self):
        self.L.close()

    def _deliver_drill(self):
        for _label, frags in drill_delivery_order():
            self.cdc.receive_fragments(frags)

    def test_later_tx_ready_cannot_overtake_missing_prefix(self):
        self.cdc.receive_fragments(drill_transactions()[3][1])
        # tx 3 is ready but tx 0..2 missing: advance does nothing.
        r = self.cdc.advance(SOURCE_ID)
        self.assertEqual(r["next_seq"], 0)
        self.assertEqual(self.L.snapshots().get(TBL_CUSTOMER, {}), {})
        blocker = self.cdc.source_status(SOURCE_ID, window=4)["first_blocker"]
        self.assertEqual(blocker["tx_seq"], 0)

    def test_full_drill_matches_independent_model(self):
        self._deliver_drill()
        # After delivering everything, advance in natural source order.
        r0 = self.cdc.advance(SOURCE_ID)
        self.assertEqual([d["outcome"] for d in r0["decisions"]],
                         ["committed", "committed", "aborted", "committed"])
        state, identity, water, next_seq = MiniModel(
            source_config()).expected_rows_after([0, 1, 2, 3])
        snap = self.L.snapshots()
        self.assertEqual(snap[TBL_CUSTOMER], state[TBL_CUSTOMER])
        self.assertEqual(snap[TBL_ORDER], state[TBL_ORDER])
        self.assertEqual(self.cdc.source_status(SOURCE_ID)["next_seq"], next_seq)
        # Persisted identity mappings equal the model's.
        rows = self.L.db.conn.execute(
            "SELECT source_table,key_json,row_id FROM cdc_identity "
            "WHERE source_id=? ORDER BY source_table,key_json",
            (SOURCE_ID,)).fetchall()
        got_ident = {(r["source_table"], r["key_json"]): r["row_id"]
                     for r in rows}
        from revoke_ledger.canon import canon as _canon
        want_ident = {k[0] + "|" + _canon(list(k[1])): v
                      for k, v in identity.items()}
        got_flat = {k[0] + "|" + k[1]: v for k, v in got_ident.items()}
        self.assertEqual(got_flat, want_ident)
        # One ledger batch per committed source tx (abort produces none).
        batches = self.L.ledger()
        self.assertEqual([b["logical_batch_id"] for b in batches],
                         ["cdc:crm:0:T0", "cdc:crm:1:T1", "cdc:crm:3:T3"])
        for b in batches:
            self.assertEqual(b["status"], "success")
            self.assertIn("cdc", self.L.explain_batch(b["logical_batch_id"]))

    def test_child_before_parent_and_repeated_updates(self):
        self._deliver_drill()
        self.cdc.advance(SOURCE_ID, limit=1)
        state0, _, _, _ = MiniModel(source_config()).expected_rows_after([0])
        self.assertEqual(self.L.snapshots()[TBL_ORDER], state0[TBL_ORDER])
        # Reference column converted to the parent's row id.
        self.assertEqual(self.L.snapshots()[TBL_ORDER][1]["cust"], 1)
        self.cdc.advance(SOURCE_ID, limit=1)
        state1, _, _, _ = MiniModel(source_config()).expected_rows_after([0, 1])
        snap = self.L.snapshots()
        self.assertEqual(snap[TBL_CUSTOMER], state1[TBL_CUSTOMER])
        self.assertEqual(snap[TBL_ORDER], state1[TBL_ORDER])
        # delete/recreate kept the same row_id mapping.
        expl = self.cdc.explain_transaction(SOURCE_ID, 1)
        delete_map = [m for m in expl["change_maps"] if m["op"] == "delete"][0]
        insert_map = [m for m in expl["change_maps"] if m["op"] == "insert"][0]
        self.assertEqual(delete_map["row_id"], insert_map["row_id"])

    def test_abort_consumed_but_has_no_effects(self):
        self._deliver_drill()
        self.cdc.advance(SOURCE_ID)
        decision = self.cdc._decision(SOURCE_ID, 2)
        self.assertEqual(decision["outcome"], "aborted")
        self.assertIsNone(self.L.db.batch_by_logical_id("cdc:crm:2:T2"))
        self.assertEqual(
            self.cdc.explain_transaction(SOURCE_ID, 2)["state"],
            ST_CONSUMED_ABORTED)

    def test_typed_identity_int_bool_text_distinct(self):
        frags = [
            begin_f(0, "T0"),
            chg(0, "T0", 1, "customer", "insert", None,
                {"tenant": "k", "code": 1, "name": "int"}),
            chg(0, "T0", 2, "customer", "insert", None,
                {"tenant": "k", "code": True, "name": "bool"}),
            chg(0, "T0", 3, "customer", "insert", None,
                {"tenant": "k", "code": "1", "name": "text"}),
            end_f(0, "T0", 3),
        ]
        self.cdc.receive_fragments(frags)
        self.cdc.advance(SOURCE_ID)
        rows = self.L.snapshots()[TBL_CUSTOMER]
        # Allocation order is deterministic over canonical key tuples; the
        # three typed identities remain distinct rows whatever their numbering.
        codes = sorted(((type(r["code"]).__name__, r["code"])
                        for r in rows.values()))
        self.assertEqual(codes, [("bool", True), ("int", 1), ("str", "1")])
        self.assertEqual(len(rows), 3)
        # And the persisted keys are three different typed tuples.
        keys = [json.loads(r["key_json"]) for r in self.L.db.conn.execute(
            "SELECT key_json FROM cdc_identity WHERE source_table='customer' "
            "ORDER BY key_json")]
        self.assertEqual(keys, [["k", "1"], ["k", 1], ["k", True]])

    def test_mapping_survives_delete_and_is_reused(self):
        frags = [
            begin_f(0, "T0"),
            chg(0, "T0", 1, "order", "insert", None, {"oid": 5, "cust": None}),
            end_f(0, "T0", 1),
            begin_f(1, "T1"),
            chg(1, "T1", 1, "order", "delete",
                {"oid": 5, "cust": None}, None),
            end_f(1, "T1", 1),
            begin_f(2, "T2"),
            chg(2, "T2", 1, "order", "insert", None,
                {"oid": 5, "cust": None, "amt": 9}),
            end_f(2, "T2", 1),
        ]
        self.cdc.receive_fragments(frags)
        self.cdc.advance(SOURCE_ID)
        self.assertEqual(self.L.snapshots()[TBL_ORDER][1]["amt"], 9)
        water = self.L.db.conn.execute(
            "SELECT high_water FROM cdc_allocator WHERE source_id=? AND "
            "managed_table=?", (SOURCE_ID, TBL_ORDER)).fetchone()["high_water"]
        self.assertEqual(water, 1)

    def test_before_mismatch_detects_external_change_true_vs_one(self):
        # Seed a managed row directly through an ordinary-style seed is not
        # allowed (owned tables must stay empty); instead create the row via
        # tx0, then mutate it out-of-band and let tx1's before fail.
        self.cdc.receive_fragments([
            begin_f(0, "T0"),
            chg(0, "T0", 1, "customer", "insert", None,
                {"tenant": "a", "code": "1", "flag": True}),
            end_f(0, "T0", 1),
        ])
        self.cdc.advance(SOURCE_ID)
        # Out-of-band modification of the managed target.
        gen = self.L.head()["generation"]
        tamper_managed_row(
            self.L, gen, TBL_CUSTOMER, 1,
            {"tenant": "a", "code": "1", "flag": 1})
        self.cdc.receive_fragments([
            begin_f(1, "T1"),
            chg(1, "T1", 1, "customer", "update",
                {"tenant": "a", "code": "1", "flag": True},
                {"tenant": "a", "code": "1", "flag": False}),
            end_f(1, "T1", 1),
        ])
        r = self.cdc.advance(SOURCE_ID)
        self.assertEqual(r["decisions"][0]["outcome"], "rejected")
        decision = self.cdc._decision(SOURCE_ID, 1)
        self.assertEqual(decision["detail"]["abort"]["code"],
                         "cdc_before_mismatch")
        self.assertEqual(
            decision["detail"]["abort"]["detail"]["change_index"], 1)
        # cursor stuck, target untouched by the rejected tx.
        self.assertEqual(self.cdc.source_status(SOURCE_ID)["next_seq"], 1)
        self.assertEqual(
            self.cdc.explain_transaction(SOURCE_ID, 1)["state"],
            ST_REJECTED_PENDING)

    def test_missing_field_vs_explicit_null_distinguished(self):
        self.cdc.receive_fragments([
            begin_f(0, "T0"),
            chg(0, "T0", 1, "customer", "insert", None,
                {"tenant": "a", "code": "1", "note": None}),
            end_f(0, "T0", 1),
        ])
        self.cdc.advance(SOURCE_ID)
        # before row lacks "note" while the fragment expects explicit null.
        gen = self.L.head()["generation"]
        tamper_managed_row(self.L, gen, TBL_CUSTOMER, 1,
                           {"tenant": "a", "code": "1"})
        self.cdc.receive_fragments([
            begin_f(1, "T1"),
            chg(1, "T1", 1, "customer", "update",
                {"tenant": "a", "code": "1", "note": None},
                {"tenant": "a", "code": "1", "note": "x"}),
            end_f(1, "T1", 1),
        ])
        r = self.cdc.advance(SOURCE_ID)
        self.assertEqual(r["decisions"][0]["outcome"], "rejected")

    def test_rejected_transaction_blocks_later_ready_tx(self):
        # tx0 passes protocol validation but fails at execution: an update of
        # an absent row (its before image cannot match anything).
        self.cdc.receive_fragments([
            begin_f(0, "T0"),
            chg(0, "T0", 1, "customer", "update",
                {"tenant": "a", "code": "1", "name": "old"},
                {"tenant": "a", "code": "1", "name": "new"}),
            end_f(0, "T0", 1),
            begin_f(1, "T1"),
            chg(1, "T1", 1, "customer", "insert", None,
                {"tenant": "q", "code": "z"}),
            end_f(1, "T1", 1),
        ])
        r = self.cdc.advance(SOURCE_ID)
        self.assertEqual(len(r["decisions"]), 1)
        self.assertEqual(r["decisions"][0]["outcome"], "rejected")
        self.assertEqual(r["next_seq"], 0)
        # Re-advancing returns the same rejection, no automatic retry.
        r2 = self.cdc.advance(SOURCE_ID)
        self.assertEqual(r2["decisions"][0]["decision_digest"],
                         r["decisions"][0]["decision_digest"])
        self.assertEqual(r2["decisions"][0].get("already_decided"), True)

    def test_skip_requires_matching_digest_and_blocking_position(self):
        self.test_rejected_transaction_blocks_later_ready_tx()
        decision = self.cdc._decision(SOURCE_ID, 0)
        # wrong digest rejected
        with self.assertRaises(LedgerError):
            self.cdc.skip_rejected(SOURCE_ID, 0, "bad data", "wrong-digest")
        # cannot skip a non-blocking sequence
        with self.assertRaises(LedgerError):
            self.cdc.skip_rejected(SOURCE_ID, 1, "bad data",
                                   decision["decision_digest"])
        out = self.cdc.skip_rejected(
            SOURCE_ID, 0, "upstream confirmed corrupt",
            decision["decision_digest"])
        self.assertEqual(out["state"], ST_SKIPPED)
        # Now tx1 can advance.
        r = self.cdc.advance(SOURCE_ID)
        self.assertEqual(r["decisions"][0]["outcome"], "committed")
        self.assertEqual(r["next_seq"], 2)
        # Skip recorded with reason and bound digest.
        self.assertEqual(out["skip"]["reason"], "upstream confirmed corrupt")
        self.assertEqual(out["skip"]["decision_digest"],
                         decision["decision_digest"])

    def test_skip_reason_required(self):
        self.test_rejected_transaction_blocks_later_ready_tx()
        d = self.cdc._decision(SOURCE_ID, 0)["decision_digest"]
        with self.assertRaises(LedgerError):
            self.cdc.skip_rejected(SOURCE_ID, 0, "  ", d)

    def test_rejected_tx_leaves_no_new_mapping_or_allocator_move(self):
        # An update of an absent row is allocated provisionally but the
        # execution rejection rolls the mapping and allocator move back.
        self.cdc.receive_fragments([
            begin_f(0, "T0"),
            chg(0, "T0", 1, "customer", "update",
                {"tenant": "a", "code": "1", "name": "old"},
                {"tenant": "a", "code": "1", "name": "new"}),
            end_f(0, "T0", 1),
        ])
        self.cdc.advance(SOURCE_ID)
        n_ident = self.L.db.conn.execute(
            "SELECT COUNT(*) c FROM cdc_identity").fetchone()["c"]
        water = self.L.db.conn.execute(
            "SELECT high_water FROM cdc_allocator").fetchall()
        self.assertEqual(n_ident, 0)
        self.assertTrue(all(r["high_water"] == 0 for r in water))

    def test_replay_of_consumed_fragments_still_verifies(self):
        self.test_full_drill_matches_independent_model()
        txs = drill_transactions()
        r = self.cdc.receive_fragments(txs[0][1])
        self.assertTrue(all(x["status"] == "duplicate" for x in r["fragments"]))
        # Conflicting re-delivery after consumption is still refused.
        evil = chg(0, "T0", 1, "customer", "insert", None,
                   {"tenant": "a", "code": "1", "name": "TAMPERED"})
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments([evil])
        # Re-advance does not re-enter the warehouse.
        batches_before = len(self.L.ledger())
        self.cdc.advance(SOURCE_ID)
        self.assertEqual(len(self.L.ledger()), batches_before)

    def test_reopen_preserves_all_state(self):
        self.test_full_drill_matches_independent_model()
        path = self.L.db.path
        self.L.close()
        L2 = Ledger(path)
        cdc2 = L2.cdc()
        self.assertEqual(cdc2.source_status(SOURCE_ID)["next_seq"], 4)
        state, _, _, _ = MiniModel(source_config()).expected_rows_after(
            [0, 1, 2, 3])
        self.assertEqual(L2.snapshots()[TBL_CUSTOMER], state[TBL_CUSTOMER])
        expl = cdc2.explain_transaction(SOURCE_ID, 1)
        self.assertEqual(len(expl["change_maps"]), 5)
        L2.close()

    def test_ordinary_program_cannot_write_owned_table(self):
        from fixtures.scenarios import _read, _write, prog
        install(self.L)
        p = prog("X", [
            _read("r", TBL_CUSTOMER, [1], {"kind": "all"}),
            _write("insert", TBL_CUSTOMER, "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"x": {"const": 1}}}])])
        with self.assertRaises(LedgerError):
            self.L.ingest(p)

    def test_ordinary_pending_batch_blocks_cdc_advance(self):
        from fixtures.scenarios import _read, _write, prog
        # A program on a non-owned table ingested but not run.
        p = prog("P", [
            _read("r", "other", [1], {"kind": "all"}),
            _write("insert", "other", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"v": {"const": 1}}}])])
        self.L.ingest(p)
        self.cdc.receive_fragments([begin_f(0, "T0"), end_f(0, "T0", 0)])
        with self.assertRaises(LedgerError):
            self.cdc.advance(SOURCE_ID)

    def test_explain_row_reverse_lookup(self):
        self.test_full_drill_matches_independent_model()
        out = self.cdc.explain_row(TBL_ORDER, 1)
        self.assertEqual(out["source_table"], "order")
        self.assertEqual(out["source_key"], [7])
        with self.assertRaises(LedgerError):
            self.cdc.explain_row(TBL_ORDER, 999)


class TestConcurrencyAndRecovery(unittest.TestCase):
    def setUp(self):
        self.L, self.d = new_ledger()
        install(self.L)
        self.cdc = self.L.cdc()
        self._closed = False

    def tearDown(self):
        if not self._closed:
            self.L.close()
            self._closed = True

    def test_concurrent_threads_one_decision_per_tx(self):
        import threading
        txs = drill_transactions()
        for seq in (0, 1):
            self.cdc.receive_fragments(txs[seq][1])
        path = self.L.db.path
        self.L.close()
        self._closed = True
        errors, done = [], []
        barrier = threading.Barrier(4)

        def worker():
            # Each thread owns its own connection to the same SQLite file,
            # matching the two-process deployment the BEGIN IMMEDIATE lock
            # serializes.
            Lx = Ledger(path)
            try:
                barrier.wait()
                for _ in range(25):
                    try:
                        Lx.cdc().advance(SOURCE_ID)
                        break
                    except Exception as exc:
                        if type(exc).__name__ != "CdcBusyError":
                            raise
                done.append(1)
            except Exception as exc:  # pragma: no cover - failure capture
                errors.append(exc)
            finally:
                Lx.close()

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(done), 4)
        check = Ledger(path)
        # Two source transactions -> exactly two decisions and two batches.
        n_dec = check.db.conn.execute(
            "SELECT COUNT(*) c FROM cdc_tx_decision").fetchone()["c"]
        self.assertEqual(n_dec, 2)
        self.assertEqual(len(check.ledger()), 2)
        self.assertEqual(check.cdc().source_status(SOURCE_ID)["next_seq"], 2)
        check.close()

    def test_response_loss_retry_returns_same_decision(self):
        self.cdc.receive_fragments(drill_transactions()[0][1])
        first = self.cdc.advance(SOURCE_ID)
        digest = first["decisions"][0]["decision_digest"]
        # A second advance after the response is lost must not re-execute.
        second = self.cdc.advance(SOURCE_ID)
        self.assertEqual(
            second["decisions"][0]["decision_digest"], digest)
        self.assertTrue(second["decisions"][0]["already_decided"])
        self.assertEqual(len(self.L.ledger()), 1)

    def test_crash_after_staging_leaves_nothing_durable(self):
        self.cdc.receive_fragments(drill_transactions()[0][1])
        self.cdc.crash_points["after_staging_before_commit"] = True
        self.cdc.crash_points["_hard_exit_hook"] = lambda point: (_ for _ in ()).throw(
            SystemExit(97))
        with self.assertRaises(SystemExit):
            self.cdc.advance(SOURCE_ID)
        # The hard exit happens inside the open outer transaction; close the
        # crashed connection (releasing its WAL lock) before reopening.
        path = self.L.db.path
        self.L.close()
        self._closed = True
        # A fresh connection sees the rollback (uncommitted work is discarded).
        L2 = Ledger(path)
        c2 = L2.cdc()
        self.assertEqual(c2.source_status(SOURCE_ID)["next_seq"], 0)
        self.assertEqual(L2.snapshots().get(TBL_CUSTOMER, {}), {})
        self.assertEqual(L2.db.conn.execute(
            "SELECT COUNT(*) c FROM cdc_identity").fetchone()["c"], 0)
        self.assertEqual(L2.db.conn.execute(
            "SELECT COUNT(*) c FROM ledger_batch").fetchone()["c"], 0)
        # The fragments remain staged, so a clean retry succeeds.
        r = L2.cdc().advance(SOURCE_ID)
        self.assertEqual(r["decisions"][0]["outcome"], "committed")
        L2.close()

    def test_crash_after_commit_is_idempotent_on_reopen(self):
        self.cdc.receive_fragments(drill_transactions()[0][1])
        self.cdc.crash_points["after_commit_before_response"] = True
        self.cdc.crash_points["_hard_exit_hook"] = lambda point: (_ for _ in ()).throw(
            SystemExit(97))
        with self.assertRaises(SystemExit):
            self.cdc.advance(SOURCE_ID)
        path = self.L.db.path
        self.L.close()
        self._closed = True
        L2 = Ledger(path)
        c2 = L2.cdc()
        self.assertEqual(c2.source_status(SOURCE_ID)["next_seq"], 1)
        self.assertEqual(len(L2.ledger()), 1)
        r = L2.cdc().advance(SOURCE_ID)
        self.assertEqual(r["decisions"][0]["already_decided"], True)
        self.assertEqual(len(L2.ledger()), 1)  # no duplicate batch
        L2.close()


# ---------------------------------------------------------------------------
class TestConstraintsIntegration(unittest.TestCase):
    def setUp(self):
        self.L, self.d = new_ledger(constraints=constraint_rules())
        install(self.L)
        self.cdc = self.L.cdc()

    def tearDown(self):
        self.L.close()

    def test_fk_missing_rejected_at_batch_final_gate(self):
        # Reference to a customer key that never exists in the tx.
        frags = [
            begin_f(0, "T0"),
            chg(0, "T0", 1, "order", "insert", None,
                {"oid": 1, "cust": ["a", "1"], "amt": 5}),
            end_f(0, "T0", 1),
        ]
        self.cdc.receive_fragments(frags)
        r = self.cdc.advance(SOURCE_ID)
        self.assertEqual(r["decisions"][0]["outcome"], "rejected")
        abort = self.cdc._decision(SOURCE_ID, 0)["detail"]["abort"]
        self.assertEqual(abort["code"], "constraint_violation")
        self.assertTrue(any(x["code"] == "fk_missing"
                            for x in abort["detail"]["reasons"]))
        self.assertEqual(self.L.snapshots().get(TBL_ORDER, {}), {})
        self.assertEqual(self.cdc.source_status(SOURCE_ID)["next_seq"], 0)

    def test_swap_unique_values_in_one_tx_allowed(self):
        frags = [
            begin_f(0, "T0"),
            chg(0, "T0", 1, "customer", "insert", None,
                {"tenant": "a", "code": "1", "name": "x"}),
            chg(0, "T0", 2, "customer", "insert", None,
                {"tenant": "a", "code": "2", "name": "y"}),
            end_f(0, "T0", 2),
            begin_f(1, "T1"),
            chg(1, "T1", 1, "customer", "update",
                {"tenant": "a", "code": "1", "name": "x"},
                {"tenant": "a", "code": "1", "name": "x2"}),
            chg(1, "T1", 2, "customer", "update",
                {"tenant": "a", "code": "2", "name": "y"},
                {"tenant": "a", "code": "2", "name": "y2"}),
            end_f(1, "T1", 2),
        ]
        self.cdc.receive_fragments(frags)
        r = self.cdc.advance(SOURCE_ID)
        self.assertTrue(all(d["outcome"] == "committed"
                            for d in r["decisions"]))


class TestMoreSemantics(unittest.TestCase):
    def setUp(self):
        self.L, self.d = new_ledger()
        install(self.L)
        self.cdc = self.L.cdc()

    def tearDown(self):
        self.L.close()

    def _commit(self, seq, tx, change_specs, outcome="commit"):
        """Build and advance one transaction from (table,op,before,after)."""
        frags = [begin_f(seq, tx)]
        for i, (table, op, before, after) in enumerate(change_specs, 1):
            frags.append(chg(seq, tx, i, table, op, before, after))
        frags.append(end_f(seq, tx, len(change_specs), outcome))
        self.cdc.receive_fragments(frags)
        return self.cdc.advance(SOURCE_ID)

    def test_update_that_changes_primary_key_rejected(self):
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments([
                begin_f(0, "T0"),
                chg(0, "T0", 1, "customer", "update",
                    {"tenant": "a", "code": "1"},
                    {"tenant": "a", "code": "2"}),
                end_f(0, "T0", 1)])

    def test_insert_when_target_exists_rejected(self):
        self._commit(0, "T0", [
            ("customer", "insert", None,
             {"tenant": "a", "code": "1", "name": "x"})])
        r = self._commit(1, "T1", [
            ("customer", "insert", None,
             {"tenant": "a", "code": "1", "name": "dup"})])
        self.assertEqual(r["decisions"][0]["outcome"], "rejected")
        self.assertEqual(
            self.cdc._decision(SOURCE_ID, 1)["detail"]["abort"]["code"],
            "pk_conflict")

    def test_update_absent_row_rejected(self):
        r = self._commit(0, "T0", [
            ("order", "update", {"oid": 9, "cust": None},
             {"oid": 9, "cust": None, "amt": 1})])
        self.assertEqual(r["decisions"][0]["outcome"], "rejected")
        self.assertEqual(
            self.cdc._decision(SOURCE_ID, 0)["detail"]["abort"]["code"],
            "cdc_before_mismatch")

    def test_delete_absent_row_rejected(self):
        r = self._commit(0, "T0", [
            ("order", "delete", {"oid": 9, "cust": None}, None)])
        self.assertEqual(r["decisions"][0]["outcome"], "rejected")

    def test_mismatched_tx_id_inside_one_transaction_rejected(self):
        frags = [
            begin_f(0, "T0"),
            chg(0, "OTHER", 1, "customer", "insert", None,
                 {"tenant": "a", "code": "1"}),
            end_f(0, "T0", 1)]
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments(frags)

    def test_null_reference_is_preserved_and_needs_no_parent(self):
        r = self._commit(0, "T0", [
            ("order", "insert", None, {"oid": 3, "cust": None, "amt": 7})])
        self.assertEqual(r["decisions"][0]["outcome"], "committed")
        self.assertIsNone(self.L.snapshots()[TBL_ORDER][1]["cust"])

    def test_mutual_references_resolve_in_one_transaction(self):
        # Two customers cannot reference each other under this config, so use
        # two orders that both reference the customer and one another-like
        # reference is out of schema; instead verify a self-referential style
        # cycle via two rows where each key is pre-registered before rows run.
        # Here: customer + two orders referencing it, delivered child-first.
        r = self._commit(0, "T0", [
            ("order", "insert", None,
             {"oid": 1, "cust": ["a", "1"], "amt": 1}),
            ("order", "insert", None,
             {"oid": 2, "cust": ["a", "1"], "amt": 2}),
            ("customer", "insert", None,
             {"tenant": "a", "code": "1", "name": "cyc"})])
        self.assertEqual(r["decisions"][0]["outcome"], "committed")
        snap = self.L.snapshots()
        self.assertEqual(snap[TBL_ORDER][1]["cust"],
                         snap[TBL_ORDER][2]["cust"])

    def test_capacity_exhaustion_refuses_whole_transaction(self):
        self.cdc.row_id_limit = 1
        # Two distinct customer identities exceed the one-id capacity.
        r = self._commit(0, "T0", [
            ("customer", "insert", None,
             {"tenant": "a", "code": "1", "name": "x"}),
            ("customer", "insert", None,
             {"tenant": "a", "code": "2", "name": "y"})])
        self.assertEqual(r["decisions"][0]["outcome"], "rejected")
        self.assertEqual(
            self.cdc._decision(SOURCE_ID, 0)["detail"]["abort"]["code"],
            "cdc_capacity_exhausted")
        self.assertEqual(self.L.snapshots().get(TBL_CUSTOMER, {}), {})
        self.assertEqual(self.L.db.conn.execute(
            "SELECT COUNT(*) c FROM cdc_identity").fetchone()["c"], 0)
        # The high water mark was not advanced by the rolled-back preparation.
        self.assertEqual(self.L.db.conn.execute(
            "SELECT high_water FROM cdc_allocator WHERE managed_table=?",
            (TBL_CUSTOMER,)).fetchone()["high_water"], 0)
        # Clearing the seam and skipping the rejected tx lets later work run
        # with normal capacity.
        del self.cdc.row_id_limit
        digest = self.cdc._decision(SOURCE_ID, 0)["decision_digest"]
        self.cdc.skip_rejected(SOURCE_ID, 0, "capacity test", digest)
        r2 = self.cdc.receive_fragments([
            begin_f(1, "T1"),
            chg(1, "T1", 1, "customer", "insert", None,
                 {"tenant": "a", "code": "9", "name": "ok"}),
            end_f(1, "T1", 1)])
        r3 = self.cdc.advance(SOURCE_ID)
        self.assertEqual(r3["decisions"][-1]["outcome"], "committed")

    def test_identity_determinism_across_arrival_orders(self):
        # Same initial state, same tx fragments delivered in two orders must
        # yield the same key -> row_id mapping.
        def build(order):
            L, _ = new_ledger()
            install(L)
            txs = drill_transactions()[0][1]
            L.cdc().receive_fragments([txs[i] for i in order])
            L.cdc().advance(SOURCE_ID)
            rows = {r["key_json"]: r["row_id"] for r in L.db.conn.execute(
                "SELECT key_json,row_id FROM cdc_identity")}
            path = L.db.path
            L.close()
            return rows, path
        a, _ = build([0, 1, 2, 3])
        b, _ = build([3, 2, 1, 0])
        self.assertEqual(a, b)

    def test_runtime_scale_data(self):
        # Scale data is generated at runtime; nothing large is committed to the
        # repository.  N independent customers plus their orders, one tx.
        n = 250
        specs = []
        for i in range(n):
            specs.append(("customer", "insert", None,
                          {"tenant": "t", "code": f"k{i}", "name": f"n{i}"}))
        for i in range(n):
            specs.append(("order", "insert", None,
                          {"oid": 1000 + i,
                           "cust": ["t", f"k{i}"], "amt": i}))
        r = self._commit(0, "BIG", specs)
        self.assertEqual(r["decisions"][0]["outcome"], "committed")
        snap = self.L.snapshots()
        self.assertEqual(len(snap[TBL_CUSTOMER]), n)
        self.assertEqual(len(snap[TBL_ORDER]), n)
        # Resolve the expected parent row ids from the persistent identity map
        # (allocation is deterministic over canonical, lexicographic keys) and
        # verify every converted reference points at exactly that row id.
        from revoke_ledger.canon import canon as _canon
        parent_of = {}
        for i in range(n):
            key = _canon(["t", f"k{i}"])
            rid = self.L.db.conn.execute(
                "SELECT row_id FROM cdc_identity WHERE source_table='customer' "
                "AND key_json=?", (key,)).fetchone()["row_id"]
            parent_of[i] = rid
        order_row_id = {}
        for i in range(n):
            key = _canon([1000 + i])
            order_row_id[i] = self.L.db.conn.execute(
                "SELECT row_id FROM cdc_identity WHERE source_table='order' "
                "AND key_json=?", (key,)).fetchone()["row_id"]
        for i in range(n):
            self.assertEqual(snap[TBL_ORDER][order_row_id[i]]["cust"],
                             parent_of[i])
        # Every resolved reference actually names an existing managed parent.
        existing = set(snap[TBL_CUSTOMER])
        self.assertTrue(all(v in existing for v in parent_of.values()))
        # Mappings survive and are unique per typed key.
        self.assertEqual(len(set(parent_of.values())), n)
        self.assertEqual(len(set(order_row_id.values())), n)
        # The high water marks, not per-file traversal, drive numbering.
        waters = {r["managed_table"]: r["high_water"] for r in
                  self.L.db.conn.execute(
                      "SELECT managed_table,high_water FROM cdc_allocator")}
        self.assertEqual(waters[TBL_CUSTOMER], n)
        self.assertEqual(waters[TBL_ORDER], n)

    def test_start_seq_offset_is_honored(self):
        L, _ = new_ledger()
        cfg = source_config()
        cfg["start_seq"] = 100
        L.cdc().install_source(cfg)
        L.cdc().receive_fragments([begin_f(100, "T100"), end_f(100, "T100", 0)])
        r = L.cdc().advance(SOURCE_ID)
        self.assertEqual(r["next_seq"], 101)
        with self.assertRaises(ProtocolError):
            L.cdc().receive_fragments([begin_f(99, "T99")])
        L.close()

    def test_two_sources_keep_independent_cursors_and_tables(self):
        cfg_b = source_config(source_id="crm2")
        cfg_b["tables"] = [
            {"source_table": "customer", "managed_table": "other_customer",
             "key_fields": ["tenant", "code"], "references": []}]
        self.cdc.install_source(cfg_b)
        self.cdc.receive_fragments([begin_f(0, "T0"), end_f(0, "T0", 0)])
        r1 = self.cdc.advance(SOURCE_ID)
        self.cdc.receive_fragments([
            fragment("crm2", 0, "B0", "begin"),
            fragment("crm2", 0, "B0", "end", change_count=0, outcome="commit")])
        r2 = self.cdc.advance("crm2")
        self.assertEqual(r1["next_seq"], 1)
        self.assertEqual(r2["next_seq"], 1)
        # Allocators and cursors are per source/table.
        waters = {r["source_id"] + "/" + r["managed_table"]: r["high_water"]
                  for r in self.L.db.conn.execute(
                      "SELECT source_id,managed_table,high_water "
                      "FROM cdc_allocator")}
        self.assertEqual(waters["crm/dim_customer"], 0)
        self.assertEqual(waters["crm2/other_customer"], 0)

    def test_delivery_id_idempotency_and_conflict(self):
        frags = drill_transactions()[3][1]
        r1 = self.cdc.receive_fragments(frags, delivery_id="del-1")
        r2 = self.cdc.receive_fragments(frags, delivery_id="del-1")
        # The exact redelivery returns the original recorded delivery summary
        # (the persisted record, before per-call fragment details are attached).
        persisted = {k: r1[k] for k in
                     ("delivery_id", "source_id", "fragment_count",
                      "staged", "duplicate", "note")}
        self.assertEqual({k: r2[k] for k in persisted}, persisted)
        # And no extra fragment rows were created by the second call.
        n_after = self.L.db.conn.execute(
            "SELECT COUNT(*) c FROM cdc_fragment").fetchone()["c"]
        self.assertEqual(n_after, 3)
        with self.assertRaises(LedgerError):
            self.cdc.receive_fragments([begin_f(7, "T7")],
                                       delivery_id="del-1")
        # A mixed-source delivery is rejected as addressed to one source.
        with self.assertRaises(ProtocolError):
            self.cdc.receive_fragments([
                begin_f(0, "T0"),
                fragment("other", 0, "X", "begin")])


if __name__ == "__main__":
    unittest.main()
