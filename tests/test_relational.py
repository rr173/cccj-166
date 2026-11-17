"""Unit and integration tests for managed-relational/v1 (stdlib unittest).

    python3 -m unittest discover -s tests -v

Expectations in this file are hand-computed from the fixtures (independent
enumeration), never produced by the product's relational operators.
"""

import copy
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fixtures import relational_scenarios as rs
from fixtures.relational_scenarios import (rprog, rread, join, project, group,
                                           rwrite, rguard, v1prog, v1read)
from revoke_ledger.errors import LedgerError, ProtocolError
from revoke_ledger.ledger import Ledger
from revoke_ledger.protocol import validate_program
from revoke_ledger.rprotocol import validate_relational_program


def new_db():
    return Ledger(os.path.join(tempfile.mkdtemp(prefix="rtest-"), "r.db"))


def seeded(rows=None):
    L = new_db()
    L.seed(rows if rows is not None else rs.settlement_seed())
    return L


def relations_of(result):
    return {r["alias"]: r for r in result["evidence"]["relations"]}


def src_ids(identity):
    """All terminal source row_ids inside a (nested) row identity."""
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


def pair_ids(pairs):
    return sorted((tuple(src_ids(p["left"])), tuple(src_ids(p["right"])))
                  for p in pairs)


class Base(unittest.TestCase):
    pass


# ---------------------------------------------------------------------------
class TestRProtocol(Base):
    def valid(self):
        return rprog("p", [
            rread("o", "orders", [1], row_id_field="oid"),
            rread("a", "attribution", [10]),
            join("o", "a", "inner", [("channel", "channel")], "j"),
            project("j", "p", [("left.amount", "amount"),
                               ("right.customer_id", "customer")]),
            group("p", "g", ["customer"],
                  [{"op": "sum", "field": "amount", "as": "total"}]),
            rwrite("insert", "customer_summary", "g", {"field": "customer"},
                   {"total": {"field": "total"}}),
        ])

    def test_valid_roundtrip(self):
        p = validate_program(self.valid())
        self.assertEqual(p["schema"], "managed-relational/v1")
        self.assertEqual(p["steps"][2]["on"], [{"left": "channel",
                                                "right": "channel"}])

    def test_validate_program_dispatches(self):
        p = validate_program(self.valid())
        self.assertEqual(p["steps"][4]["op"], "group")

    def test_v1_program_still_validates(self):
        p = {"logical_batch_id": "v1", "schema": "managed-batch/v1",
             "steps": [{"op": "read", "alias": "r",
                        "select": {"table": "t", "where": {"kind": "all"}},
                        "map": [{"row_id": 1, "table": "t"}]}]}
        self.assertEqual(validate_program(p)["schema"], "managed-batch/v1")

    def test_rejects_unknown_toplevel_key(self):
        p = self.valid()
        p["sql"] = "DROP TABLE x"
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_unknown_op(self):
        p = self.valid()
        p["steps"].append({"op": "exec", "sql": "select 1"})
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_duplicate_alias(self):
        p = self.valid()
        p["steps"][1]["alias"] = "o"
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_forward_relation(self):
        p = self.valid()
        p["steps"][2]["right"] = "g"
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_empty_and_oversized_on(self):
        p = self.valid()
        p["steps"][2]["on"] = []
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)
        p = self.valid()
        p["steps"][2]["on"] = [{"left": "a", "right": "b"}] * 9
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_projection_name_conflict(self):
        p = self.valid()
        p["steps"][3]["select"] = [{"from": "left.amount", "as": "x"},
                                   {"from": "left.oid", "as": "x"}]
        with self.assertRaises(ProtocolError) as ctx:
            validate_relational_program(p)
        self.assertIn("name conflict", str(ctx.exception))

    def test_rejects_undefined_field_on_closed_schema(self):
        p = self.valid()
        p["steps"][4]["aggregates"] = [{"op": "sum", "field": "nosuch",
                                        "as": "total"}]
        with self.assertRaises(ProtocolError) as ctx:
            validate_relational_program(p)
        self.assertIn("undefined field", str(ctx.exception))

    def test_rejects_unqualified_field_on_join(self):
        p = self.valid()
        p["steps"][3]["select"] = [{"from": "amount", "as": "amount"}]
        with self.assertRaises(ProtocolError) as ctx:
            validate_relational_program(p)
        self.assertIn("left.", str(ctx.exception))

    def test_rejects_group_key_aggregate_conflict(self):
        p = self.valid()
        p["steps"][4]["aggregates"] = [{"op": "count_star", "as": "customer"}]
        with self.assertRaises(ProtocolError) as ctx:
            validate_relational_program(p)
        self.assertIn("name conflict", str(ctx.exception))

    def test_rejects_delete_with_values(self):
        p = self.valid()
        w = p["steps"][5]
        w["effect"] = "delete"
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_row_count_guard_with_field(self):
        p = rprog("p", [rread("o", "orders", [1]),
                        rguard("row_count", "o", field="amount", eq=1)])
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_non_int_target_const(self):
        p = self.valid()
        p["steps"][5]["target"] = {"const": "700"}
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)
        p = self.valid()
        p["steps"][5]["target"] = {"const": True}
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_duplicate_map_identity(self):
        p = self.valid()
        p["steps"][0]["map"].append({"row_id": 1, "table": "orders"})
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_row_id_field_with_dot(self):
        p = self.valid()
        p["steps"][0]["row_id_field"] = "a.b"
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_bad_aggregate(self):
        p = self.valid()
        p["steps"][4]["aggregates"] = [{"op": "avg", "field": "amount",
                                        "as": "a"}]
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_rejects_unknown_value_expr(self):
        p = self.valid()
        p["steps"][5]["values"] = {"total": {"mul": [{"field": "total"},
                                                     {"const": 2}]}}
        with self.assertRaises(ProtocolError):
            validate_relational_program(p)

    def test_open_read_allows_any_simple_field(self):
        p = rprog("p", [rread("o", "orders", [1]),
                        rguard("int_sum", "o", field="anything", ge=0)])
        self.assertEqual(validate_relational_program(p)["steps"][1]["test"],
                         "int_sum")


# ---------------------------------------------------------------------------
class TestRExecution(Base):
    def test_inner_join_duplicate_contributions(self):
        """2 left x 3 right sharing a key -> 6 inner results, no dedup."""
        L = seeded({"t1": {1: {"k": "x", "v": 1}, 2: {"k": "x", "v": 2}},
                    "t2": {10: {"k": "x"}, 11: {"k": "x"}, 12: {"k": "x"}},
                    "out": {}})
        r = L.submit(rprog("J", [
            rread("l", "t1", [1, 2], row_id_field="lid"),
            rread("r", "t2", [10, 11, 12], row_id_field="rid"),
            join("l", "r", "inner", [("k", "k")], "j"),
        ]))
        self.assertEqual(r["status"], "success")
        j = relations_of(r)["j"]
        self.assertEqual(j["row_count"], 6)
        self.assertEqual(pair_ids(j["detail"]["pairs"]),
                         [((1,), (10,)), ((1,), (11,)), ((1,), (12,)),
                          ((2,), (10,)), ((2,), (11,)), ((2,), (12,))])
        L.close()

    def test_left_join_unmatched_nulls(self):
        L = seeded()
        r = L.submit(rs.prog_settle_left())
        self.assertEqual(r["status"], "success")
        j = relations_of(r)["j"]
        self.assertEqual([u[2] for u in j["detail"]["unmatched_left"]],
                         rs.EXPECTED_LEFT["unmatched_left"])
        # Unmatched left rows carry null right-side fields.
        proj = relations_of(r)["p"]
        self.assertEqual(proj["row_count"], 6)
        self.assertEqual(L.snapshots()["order_enriched"],
                         rs.EXPECTED_LEFT["enriched"])
        L.close()

    def test_typed_keys_int_text_bool(self):
        """int 1, text '1' and bool true are three different keys."""
        L = seeded({"t1": {1: {"k": 1}, 2: {"k": "1"}, 3: {"k": True}},
                    "t2": {10: {"k": 1}, 11: {"k": "1"}, 12: {"k": True}},
                    "out": {}})
        r = L.submit(rprog("T", [
            rread("l", "t1", [1, 2, 3], row_id_field="lid"),
            rread("r", "t2", [10, 11, 12], row_id_field="rid"),
            join("l", "r", "inner", [("k", "k")], "j"),
        ]))
        j = relations_of(r)["j"]
        self.assertEqual(pair_ids(j["detail"]["pairs"]),
                         [((1,), (10,)), ((2,), (11,)), ((3,), (12,))])
        L.close()

    def test_null_and_missing_key_components_never_match(self):
        L = seeded()
        r = L.submit(rs.prog_settle_composite())
        self.assertEqual(r["status"], "success")
        j = relations_of(r)["j"]
        got = sorted((p["left"][2], p["right"][2]) for p in j["detail"]["pairs"])
        self.assertEqual(got, rs.EXPECTED_COMPOSITE["pairs"])
        self.assertEqual([u[2] for u in j["detail"]["unmatched_left"]],
                         rs.EXPECTED_COMPOSITE["unmatched_left"])
        self.assertEqual([u[2] for u in j["detail"]["unmatched_right"]],
                         rs.EXPECTED_COMPOSITE["unmatched_right"])
        self.assertEqual(L.snapshots()["shipment_enriched"],
                         rs.EXPECTED_COMPOSITE["enriched"])
        L.close()

    def test_join_join_group_chain(self):
        L = seeded()
        r = L.submit(rs.prog_settle_chain())
        self.assertEqual(r["status"], "success")
        rels = relations_of(r)
        self.assertEqual(rels["j2"]["row_count"], 6)
        groups = {tuple(c["value"] for c in g["key"]): g["values"]
                  for g in rels["g"]["detail"]["groups"]}
        want = {k: {"n": v["n"], "total": v["total"]}
                for k, v in rs.EXPECTED_CHAIN["groups"].items()}
        self.assertEqual(groups, want)
        self.assertEqual(L.snapshots()["customer_stats"],
                         rs.EXPECTED_CHAIN["stats"])
        L.close()

    def test_projection_renames_and_nulls_missing(self):
        L = seeded({"t": {1: {"a": 5}}, "out": {}})
        r = L.submit(rprog("P", [
            rread("t", "t", [1]),
            project("t", "p", [("a", "x"), ("missing", "y")]),
            rwrite("insert", "out", "p", {"const": 1},
                   {"x": {"field": "x"}, "y": {"field": "y"}}),
        ]))
        self.assertEqual(r["status"], "success")
        self.assertEqual(L.snapshots()["out"][1], {"x": 5, "y": None})
        L.close()

    def test_group_keys_are_typed(self):
        L = seeded({"t": {1: {"k": 1}, 2: {"k": "1"}, 3: {"k": True},
                          4: {"k": 1}}, "out": {}})
        r = L.submit(rprog("G", [
            rread("t", "t", [1, 2, 3, 4]),
            group("t", "g", ["k"], [{"op": "count_star", "as": "n"}]),
        ]))
        groups = relations_of(r)["g"]["detail"]["groups"]
        got = sorted((g["key"][0]["type"], json.dumps(g["key"][0]["value"]),
                      g["values"]["n"]) for g in groups)
        self.assertEqual(got, [("bool", "true", 1), ("int", "1", 2),
                               ("text", '"1"', 1)])
        L.close()

    def test_group_null_and_missing_unify(self):
        L = seeded({"t": {1: {"k": None, "v": 1}, 2: {"v": 2},
                          3: {"k": None, "v": 3}}, "out": {}})
        r = L.submit(rprog("G", [
            rread("t", "t", [1, 2, 3]),
            group("t", "g", ["k"], [{"op": "count_star", "as": "n"},
                                    {"op": "sum", "field": "v", "as": "s"}]),
        ]))
        groups = relations_of(r)["g"]["detail"]["groups"]
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["key"], [{"type": "null", "value": None}])
        self.assertEqual(groups[0]["values"], {"n": 3, "s": 6})
        self.assertEqual(len(groups[0]["members"]), 3)
        L.close()

    def test_empty_input_global_vs_keyed(self):
        L = seeded()
        r = L.submit(rs.prog_settle_empty())
        self.assertEqual(r["status"], "success")
        rels = relations_of(r)
        g = rels["g"]["detail"]["groups"]
        self.assertEqual(len(g), 1)
        self.assertEqual(g[0]["members"], [])
        self.assertEqual(g[0]["values"], {"n": 0, "na": 0, "total": None,
                                          "lo": None, "hi": None})
        self.assertEqual(rels["gk"]["row_count"], 0)
        self.assertEqual(L.snapshots()["stats"][1],
                         rs.EXPECTED_EMPTY["stats"][1])
        L.close()

    def test_count_field_skips_null(self):
        L = seeded({"t": {1: {"v": None}, 2: {"v": 3}, 3: {}}, "out": {}})
        r = L.submit(rprog("C", [
            rread("t", "t", [1, 2, 3]),
            group("t", "g", [], [{"op": "count", "field": "v", "as": "c"},
                                 {"op": "count_star", "as": "n"}]),
        ]))
        self.assertEqual(relations_of(r)["g"]["detail"]["groups"][0]["values"],
                         {"c": 1, "n": 3})
        L.close()

    def test_sum_all_null_is_null(self):
        L = seeded({"t": {1: {"v": None}, 2: {}}, "out": {}})
        r = L.submit(rprog("S", [
            rread("t", "t", [1, 2]),
            group("t", "g", [], [{"op": "sum", "field": "v", "as": "s"}]),
        ]))
        self.assertIsNone(relations_of(r)["g"]["detail"]["groups"][0]
                          ["values"]["s"])
        L.close()

    def test_sum_exact_cancellation_not_rejected(self):
        """2**62 + 2**62 - 2**62 == 2**62: only the final result is checked."""
        L = seeded()
        r = L.submit(rs.prog_settle_cancel())
        self.assertEqual(r["status"], "success")
        self.assertEqual(L.snapshots()["stats"][2]["total"],
                         rs.EXPECTED_CANCEL["total"])
        L.close()

    def test_sum_final_overflow_rejects_batch(self):
        L = seeded()
        r = L.submit(rs.prog_settle_overflow())
        self.assertEqual(r["status"], "failed")
        abort = r["abort"]
        self.assertEqual(abort["code"], "type_error")
        self.assertEqual(abort["step_index"], 1)
        self.assertEqual(abort["detail"]["aggregate"], "total")
        self.assertEqual(abort["detail"]["group_key"], [])
        self.assertEqual(abort["detail"]["result"], 2 ** 63)
        self.assertEqual(L.snapshots().get("stats", {}), {})
        L.close()

    def test_sum_rejects_bool_and_text(self):
        L = seeded({"t": {1: {"v": True}}, "out": {}})
        r = L.submit(rprog("S", [
            rread("t", "t", [1]),
            group("t", "g", [], [{"op": "sum", "field": "v", "as": "s"}]),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["detail"]["offending"],
                         {"type": "bool", "value": True})
        L.close()
        L = seeded()
        r = L.submit(rs.prog_settle_typeerr())
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["step_index"], 1)
        self.assertEqual(r["abort"]["detail"]["aggregate"], "total")
        self.assertEqual(r["abort"]["detail"]["offending"]["type"], "text")
        L.close()

    def test_min_max_int_text_and_type_errors(self):
        L = seeded({"t": {1: {"v": 3, "w": "b"}, 2: {"v": 7, "w": "a"},
                          3: {"v": None, "w": None}}, "out": {}})
        r = L.submit(rprog("M", [
            rread("t", "t", [1, 2, 3]),
            group("t", "g", [], [{"op": "min", "field": "v", "as": "lo"},
                                 {"op": "max", "field": "v", "as": "hi"},
                                 {"op": "min", "field": "w", "as": "wl"},
                                 {"op": "max", "field": "w", "as": "wh"}]),
        ]))
        self.assertEqual(relations_of(r)["g"]["detail"]["groups"][0]["values"],
                         {"lo": 3, "hi": 7, "wl": "a", "wh": "b"})
        L.close()
        # mixed types in one group -> located type error
        L = seeded({"t": {1: {"v": 1}, 2: {"v": "x"}}, "out": {}})
        r = L.submit(rprog("M", [
            rread("t", "t", [1, 2]),
            group("t", "g", [], [{"op": "min", "field": "v", "as": "m"}]),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["detail"]["types"], ["int", "text"])
        L.close()
        # bool is not a min/max domain
        L = seeded({"t": {1: {"v": True}}, "out": {}})
        r = L.submit(rprog("M", [
            rread("t", "t", [1]),
            group("t", "g", [], [{"op": "max", "field": "v", "as": "m"}]),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["detail"]["types"], ["bool"])
        L.close()

    def test_write_target_from_field_and_expr(self):
        L = seeded()
        r = L.submit(rs.prog_settle_inner())
        self.assertEqual(r["status"], "success")
        self.assertEqual(L.snapshots()["customer_summary"],
                         rs.EXPECTED_INNER["summary"])
        # target from an add/sub expression over a declared integer field
        L2 = seeded({"t": {1: {"base": 40}}, "out": {}})
        r2 = L2.submit(rprog("W", [
            rread("t", "t", [1]),
            project("t", "p", [("base", "base")]),
            rwrite("insert", "out", "p",
                   {"add": [{"field": "base"}, {"const": 2}]},
                   {"v": {"const": 1}}),
        ]))
        self.assertEqual(r2["status"], "success")
        self.assertEqual(L2.snapshots()["out"][42], {"v": 1})
        L.close(); L2.close()

    def test_duplicate_target_rejected_even_with_equal_content(self):
        L = seeded()
        head = L.head()
        r = L.submit(rs.prog_settle_collide())
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "pk_conflict")
        self.assertEqual(r["abort"]["detail"]["duplicates"], [700])
        self.assertEqual(len(r["abort"]["detail"]["sources"]), 2)
        self.assertEqual(L.head(), head)          # nothing committed
        self.assertEqual(L.snapshots().get("stats", {}), {})
        L.close()

    def test_insert_existing_and_delete_missing(self):
        L = seeded({"t": {1: {"v": 1}, 5: {"v": 9}}, "out": {}})
        r = L.submit(rprog("I", [
            rread("t", "t", [1]),
            rwrite("insert", "t", "t", {"const": 5}, {"v": {"const": 0}}),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "pk_conflict")
        r = L.submit(rprog("D", [
            rread("t", "t", [1]),
            rwrite("delete", "t", "t", {"const": 6}),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "delete_missing")
        self.assertEqual(L.snapshots()["t"][5], {"v": 9})
        L.close()

    def test_null_target_is_type_error(self):
        L = seeded({"t": {1: {"c": None}}, "out": {}})
        r = L.submit(rprog("N", [
            rread("t", "t", [1]),
            project("t", "p", [("c", "c")]),
            rwrite("insert", "out", "p", {"field": "c"}, {"v": {"const": 1}}),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "type_error")
        L.close()

    def test_guard_failure_rolls_back_earlier_writes(self):
        L = seeded({"t": {1: {"v": 1}}, "out": {}, "out2": {}})
        head = L.head()
        r = L.submit(rprog("G", [
            rread("t", "t", [1]),
            rwrite("insert", "out", "t", {"const": 1}, {"v": {"const": 1}}),
            rguard("int_sum", "t", field="v", ge=100),
            rwrite("insert", "out2", "t", {"const": 1}, {"v": {"const": 1}}),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "guard_failed")
        self.assertEqual(L.head(), head)
        self.assertEqual(L.snapshots().get("out", {}), {})
        self.assertEqual(L.snapshots().get("out2", {}), {})
        L.close()

    def test_guard_values_recorded(self):
        L = seeded()
        r = L.submit(rs.prog_settle_inner())
        g = r["evidence"]["guards"][0]
        self.assertEqual(g["test"], "row_count")
        self.assertEqual(g["relation"], "g")
        self.assertEqual(g["value"], 3)
        self.assertTrue(g["passed"])
        L.close()

    def test_map_permutation_keeps_identity_and_results(self):
        def build(order_o, order_a):
            p = rs.prog_settle_inner()
            p["steps"][0]["map"] = [p["steps"][0]["map"][i] for i in order_o]
            p["steps"][1]["map"] = [p["steps"][1]["map"][i] for i in order_a]
            return p
        L1 = seeded(); L2 = seeded()
        r1 = L1.submit(build([0, 1, 2, 3, 4, 5], [0, 1, 2, 3]))
        r2 = L2.submit(build([5, 3, 4, 1, 0, 2], [2, 0, 3, 1]))
        self.assertEqual(r1["status"], r2["status"], "success")
        rels1 = relations_of(r1); rels2 = relations_of(r2)
        for alias in ("o", "a", "j", "p", "g"):
            self.assertEqual(rels1[alias]["digest"], rels2[alias]["digest"],
                             f"relation {alias} digest changed by map order")
        self.assertEqual(rels1["j"]["detail"]["pairs"],
                         rels2["j"]["detail"]["pairs"])
        self.assertEqual(rels1["g"]["detail"]["groups"],
                         rels2["g"]["detail"]["groups"])
        self.assertEqual(L1.snapshots(), L2.snapshots())
        L1.close(); L2.close()

    def test_relation_snapshot_is_immutable_within_batch(self):
        L = seeded({"t": {1: {"v": 10}}, "out": {}})
        r = L.submit(rprog("S", [
            rread("r1", "t", [1]),
            rwrite("replace", "t", "r1", {"const": 1},
                   {"v": {"const": 999}}),
            rread("r2", "t", [1]),
        ]))
        self.assertEqual(r["status"], "success")
        rels = relations_of(r)
        self.assertNotEqual(rels["r1"]["digest"], rels["r2"]["digest"])
        reads = {x["alias"]: x for x in r["evidence"]["reads"]}
        self.assertEqual(json.loads(reads["r1"]["classification"][0]["row"]),
                         {"v": 10})
        self.assertEqual(json.loads(reads["r2"]["classification"][0]["row"]),
                         {"v": 999})
        L.close()

    def test_join_result_limit(self):
        n = 320                       # 320 x 320 = 102400 > 100000
        L = seeded({"t1": {i: {"k": 1} for i in range(1, n + 1)},
                    "t2": {i: {"k": 1} for i in range(1, n + 1)},
                    "out": {}})
        r = L.submit(rprog("L", [
            rread("a", "t1", list(range(1, n + 1))),
            rread("b", "t2", list(range(1, n + 1))),
            join("a", "b", "inner", [("k", "k")], "j"),
        ]))
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "limit_exceeded")
        L.close()


# ---------------------------------------------------------------------------
class TestRConstraints(Base):
    def constrained(self):
        L = new_db()
        L.install_constraints(rs.SETTLEMENT_RULES)
        L.seed(rs.settlement_seed())
        return L

    def test_fk_violation_rejects_whole_relational_batch(self):
        L = self.constrained()
        head = L.head()
        r = L.submit(rs.prog_settle_fk_violation())
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["code"], "constraint_violation")
        reasons = r["abort"]["reasons"]
        self.assertEqual({x["code"] for x in reasons}, {"fk_missing"})
        self.assertEqual({x["referenced_row_id"] for x in reasons},
                         {1101, 1102, 1103})
        self.assertEqual(L.head(), head)
        self.assertEqual(L.snapshots().get("audit_summary", {}), {})
        L.close()

    def test_unique_violation_rejects_relational_batch(self):
        L = self.constrained()
        r = L.submit(rs.prog_settle_unique_violation())
        self.assertEqual(r["status"], "failed")
        self.assertEqual(r["abort"]["reasons"][0]["code"], "unique_violation")
        self.assertEqual(L.snapshots().get("order_enriched", {}), {})
        L.close()

    def test_relational_delete_cascades(self):
        rules = {"schema": "managed-constraints/v1", "rules": [
            {"name": "acct_owner_fk", "kind": "foreign_key",
             "child": "account", "field": "owner_id", "parent": "owner",
             "on_delete": "CASCADE"}]}
        L = new_db()
        L.install_constraints(rules)
        L.seed({"owner": {1: {"c": "a"}}, "account": {7: {"owner_id": 1}},
                "out": {}})
        r = L.submit(rprog("D", [
            rread("o", "owner", [1]),
            rwrite("delete", "owner", "o", {"const": 1}),
        ]))
        self.assertEqual(r["status"], "success")
        cons = r["evidence"]["constraints"]
        self.assertEqual(cons["implicit_deletes"], [["account", 7]])
        self.assertEqual(L.snapshots().get("account", {}), {})
        L.close()


# ---------------------------------------------------------------------------
class TestRRevocation(Base):
    def build(self):
        L = seeded(rs.revocation_seed())
        for b in rs.revocation_batches():
            L.submit(b)
        return L

    def test_full_revocation_flow(self):
        L = self.build()
        plan = L.plan_revocation("rev-1", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertEqual(m["PROC"]["disposition"], "Changed")
        self.assertEqual(m["G"]["disposition"], "Blocked")
        self.assertEqual(m["G"]["reason_code"], "blocked_guard")
        self.assertEqual(m["Z"]["reuse_kind"], "skipped_independent")
        res = L.apply_plan(plan["plan_id"])
        snap = L.snapshots()
        self.assertEqual(snap["customer_summary"],
                         rs.EXPECTED_REV_AFTER["customer_summary"])
        self.assertEqual(snap["order_enriched"],
                         rs.EXPECTED_REV_AFTER["order_enriched"])
        self.assertEqual(snap.get("stats", {}), {})
        self.assertEqual(snap["gamma"], {100: {"value": 7}})
        self.assertEqual(res["replayed_batch_ords"], [m["PROC"]["batch_ord"]])
        self.assertEqual(res["skipped_independent_batch_ords"],
                         [m["Z"]["batch_ord"]])
        self.assertEqual(res["blocked_batch_ords"], [m["G"]["batch_ord"]])
        L.close()

    def test_summary_is_rederived_not_patched(self):
        """The replay recomputes from read maps; it does not reuse old rows."""
        L = self.build()
        plan = L.plan_revocation("rev-1", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        derived = m["PROC"]["detail"]["replay_derived"]
        jl = [r for r in derived if r["alias"] == "jl"][0]
        self.assertEqual([u for u in jl["detail"]["unmatched_left"]], [])
        got = sorted((p["left"][2], p["right"][2])
                     for p in jl["detail"]["pairs"])
        self.assertEqual(got, rs.EXPECTED_REV_AFTER["proc_left_pairs"])
        g = [r for r in derived if r["alias"] == "g"][0]
        groups = {x["key"][0]["value"]: x["values"]
                  for x in g["detail"]["groups"]}
        self.assertEqual(groups, {101: {"n": 2, "total": 150},
                                  102: {"n": 1, "total": 70}})
        L.close()

    def test_relational_batches_are_never_skip_eligible(self):
        L = self.build()
        plan = L.plan_revocation("rev-1", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        elig = plan["skip_eligibility"][str(m["PROC"]["batch_ord"])]
        self.assertFalse(elig["eligible"])
        self.assertIn("conservatively replayed", elig["why"][0])
        L.close()

    def test_v1_partition_batch_still_skips_with_trace(self):
        L = self.build()
        plan = L.plan_revocation("rev-1", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        res = L.apply_plan(plan["plan_id"])
        trace = L.db.generation_info(res["new_generation"])["trace"]["events"][0]
        self.assertIn(m["Z"]["batch_ord"], trace["skipped_batch_ords"])
        self.assertNotIn(m["Z"]["batch_ord"], trace["executed_batch_ords"])
        self.assertIn(m["PROC"]["batch_ord"], trace["executed_batch_ords"])
        L.close()

    def test_derived_explanations_survive_reopen(self):
        L = self.build()
        plan = L.plan_revocation("rev-1", "T")
        L.apply_plan(plan["plan_id"])
        before = L.explain_derived("PROC")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        derived_before = m["PROC"]["detail"]["replay_derived"]
        path = L.db.path
        L.close()
        L2 = Ledger(path)
        after = L2.explain_derived("PROC")
        self.assertEqual(before, after)
        # Original evidence: order 3 was unmatched (null-padded left row).
        jl_orig = [j for j in after["joins"] if j["alias"] == "jl"][0]
        self.assertEqual([u[2] for u in jl_orig["unmatched_left"]], [3])
        # New-generation derived evidence: order 3 matched the restored row.
        plan2 = L2.plan_view(plan["plan_id"])
        m2 = {x["logical_batch_id"]: x for x in plan2["mapping"]}
        self.assertEqual(m2["PROC"]["detail"]["replay_derived"],
                         derived_before)
        expl = L2.explain_revocation("rev-1")
        self.assertIsNotNone(expl["batches"]["PROC"]["derived_relations"])
        L2.close()

    def test_failed_relational_batch_is_historical_only(self):
        L = seeded(rs.revocation_seed())
        batches = rs.revocation_batches()
        L.submit(batches[0])                    # T
        L.submit(batches[1])                    # PROC
        fail = rprog("F", [
            rread("s", "customer_summary", [101]),
            rguard("row_count", "s", eq=99),
        ])
        r = L.submit(fail)
        self.assertEqual(r["status"], "failed")
        plan = L.plan_revocation("rev-f", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertEqual(m["F"]["reason_code"], "blocked_historical_failure")
        self.assertEqual(m["F"]["disposition"], "Blocked")
        L.close()

    def test_guard_blocked_means_zero_writes(self):
        L = self.build()
        plan = L.plan_revocation("rev-1", "T")
        res = L.apply_plan(plan["plan_id"])
        self.assertEqual(res["blocked_reasons"],
                         {str(res["blocked_batch_ords"][0]): "blocked_guard"})
        self.assertEqual(L.snapshots().get("stats", {}), {})
        L.close()

    def test_relational_batch_as_revocation_target(self):
        L = self.build()
        plan = L.plan_revocation("rev-proc", "PROC")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertEqual(m["PROC"]["disposition"], "Superseded")
        res = L.apply_plan(plan["plan_id"])
        snap = L.snapshots()
        self.assertEqual(snap.get("customer_summary", {}), {})
        self.assertEqual(snap.get("order_enriched", {}), {})
        # G re-derives on the empty summary: the global aggregate sum is null.
        self.assertEqual(m["G"]["disposition"], "Changed")
        self.assertEqual(snap["stats"], {1: {"grand": None}})
        self.assertEqual(snap["gamma"], {100: {"value": 7}})
        L.close()

    def test_relational_upstream_of_target_is_kept(self):
        L = seeded(rs.revocation_seed())
        batches = rs.revocation_batches()
        L.submit(batches[1])                    # PROC before T
        L.submit(batches[0])                    # T
        plan = L.plan_revocation("rev-up", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        self.assertEqual(m["PROC"]["disposition"], "Kept")
        self.assertEqual(m["PROC"]["reason_code"], "upstream_of_target")
        L.close()

    def test_relational_target_influence_covers_full_read_scope(self):
        L = self.build()
        plan = L.plan_revocation("rev-inf", "PROC")
        inf = plan["influence"]
        # attribution 11 was absent at execution time but is still scope.
        self.assertEqual(inf["read_scope_keys"]["attribution"], [10, 11])
        self.assertEqual(inf["read_scope_keys"]["orders"], [1, 2, 3])
        self.assertEqual(inf["write_keys"]["customer_summary"], [101])
        L.close()

    def test_relational_partition_declaration_does_not_enable_skip(self):
        L = seeded(rs.revocation_seed())
        batches = rs.revocation_batches()
        batches[1]["partition"] = {"table": "customer_summary",
                                   "ranges": [[1, 999]]}
        for b in batches:
            L.submit(b)
        plan = L.plan_revocation("rev-part", "T")
        m = {x["logical_batch_id"]: x for x in plan["mapping"]}
        elig = plan["skip_eligibility"][str(m["PROC"]["batch_ord"])]
        self.assertFalse(elig["eligible"])
        self.assertIn("conservatively replayed", elig["why"][0])
        self.assertEqual(elig["partition"]["table"], "customer_summary")
        L.close()


class TestCommittedFixtures(Base):
    """The committed CLI JSON fixtures stay in sync with the Python builders."""

    FX = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "fixtures", "relational")

    def _load(self, name):
        with open(os.path.join(self.FX, name), "r", encoding="utf-8") as fh:
            return json.load(fh)

    def test_seed_fixture_matches(self):
        want = {t: {str(k): v for k, v in rows.items()}
                for t, rows in rs.settlement_seed().items()}
        self.assertEqual(self._load("seed.json"), want)

    def test_constraints_fixture_matches(self):
        self.assertEqual(self._load("constraints.json"), rs.SETTLEMENT_RULES)

    def test_program_fixtures_match(self):
        self.assertEqual(self._load("settle_inner.json"),
                         rs.prog_settle_inner())
        self.assertEqual(self._load("settle_left.json"),
                         rs.prog_settle_left())
        self.assertEqual(self._load("settle_fk.json"),
                         rs.prog_settle_fk_violation())
        self.assertEqual(self._load("settle_collide.json"),
                         rs.prog_settle_collide())

    def test_committed_programs_validate(self):
        for name in ("settle_inner.json", "settle_left.json",
                     "settle_fk.json", "settle_collide.json"):
            self.assertEqual(
                validate_program(self._load(name))["schema"],
                "managed-relational/v1")


if __name__ == "__main__":
    unittest.main()
