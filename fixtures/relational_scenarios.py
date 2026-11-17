"""Fixtures for the managed-relational/v1 settlement (入仓加工) drills.

Programs are plain JSON-able data; nothing here executes except through the
validated interpreter.  Every EXPECTED_* constant below is hand-computed from
the fixture rows (independent enumeration), never produced by the product's
relational operators -- the drills assert product evidence against them.
"""

INT62 = 2 ** 62
INT64_MAX = 2 ** 63 - 1


def rprog(lid, steps, partition=None):
    p = {"logical_batch_id": lid, "schema": "managed-relational/v1",
         "steps": steps}
    if partition is not None:
        p["partition"] = partition
    return p


def rread(alias, table, ids, where=None, row_id_field=None):
    st = {"op": "read", "alias": alias,
          "select": {"table": table, "where": where or {"kind": "all"}},
          "map": [{"row_id": i, "table": table} for i in ids]}
    if row_id_field is not None:
        st["row_id_field"] = row_id_field
    return st


def join(left, right, how, on, alias):
    return {"op": "join", "left": left, "right": right, "how": how,
            "on": [{"left": l, "right": r} for l, r in on], "alias": alias}


def project(src, alias, select):
    return {"op": "project", "from": src, "alias": alias,
            "select": [{"from": f, "as": a} for f, a in select]}


def group(src, alias, keys, aggregates):
    return {"op": "group", "from": src, "alias": alias, "keys": keys,
            "aggregates": aggregates}


def rwrite(effect, table, src, target, values=None):
    st = {"op": "write", "effect": effect, "table": table, "from": src,
          "target": target}
    if values is not None:
        st["values"] = values
    return st


def rguard(test, relation, field=None, **bounds):
    st = {"op": "guard", "test": test, "relation": relation}
    if field is not None:
        st["field"] = field
    st.update(bounds)
    return st


def v1prog(lid, steps, partition=None):
    p = {"logical_batch_id": lid, "schema": "managed-batch/v1", "steps": steps}
    if partition is not None:
        p["partition"] = partition
    return p


def v1read(alias, table, ids):
    return {"op": "read", "alias": alias,
            "select": {"table": table, "where": {"kind": "all"}},
            "map": [{"row_id": i, "table": table} for i in ids]}


# ---------------------------------------------------------------------------
# Main settlement seed: orders -> channel attribution -> customer summaries.
#
# Channel keys deliberately mix types: order 5 has TEXT channel "1", order 6
# has INT channel 1; attribution 13 has INT channel 1.  "1" != 1 != true.
# Attribution 10 and 11 both cover "web" (one channel, many attributions).
# ---------------------------------------------------------------------------
def settlement_seed():
    return {
        "orders": {
            1: {"channel": "web", "amount": 100},
            2: {"channel": "web", "amount": 50},
            3: {"channel": "app", "amount": 70},
            4: {"channel": "none", "amount": 9},
            5: {"channel": "1", "amount": 20},
            6: {"channel": 1, "amount": 30},
        },
        "attribution": {
            10: {"channel": "web", "customer_id": 101},
            11: {"channel": "web", "customer_id": 102},
            12: {"channel": "app", "customer_id": 101},
            13: {"channel": 1, "customer_id": 103},
        },
        "customers": {
            101: {"name": "acme"},
            102: {"name": "beta"},
            103: {"name": "cora"},
        },
        "shipments": {
            1: {"lane": "sea", "region": "cn", "kg": 40},
            2: {"lane": "sea", "region": None, "kg": 41},
            3: {"lane": "sea", "kg": 42},
            4: {"lane": "air", "region": "cn", "kg": 10},
        },
        "lane_owner": {
            20: {"lane": "sea", "region": "cn", "owner": "ops"},
            21: {"lane": "sea", "region": None, "owner": "null-owner"},
            22: {"lane": "air", "region": "cn", "owner": "air-ops"},
        },
        "adjustments": {1: {"amt": INT62}, 2: {"amt": INT62},
                        3: {"amt": -INT62}},
        "overflows": {1: {"amt": INT64_MAX}, 2: {"amt": 1}},
        "customer_summary": {},
        "order_enriched": {},
        "shipment_enriched": {},
        "customer_stats": {},
        "stats": {},
        "audit_summary": {},
    }


# --- P1: inner join + group + write summaries (one-to-many, typed keys) ----
def prog_settle_inner():
    return rprog("SETTLE-INNER", [
        rread("o", "orders", [1, 2, 3, 4, 5, 6], row_id_field="oid"),
        rread("a", "attribution", [10, 11, 12, 13]),
        join("o", "a", "inner", [("channel", "channel")], "j"),
        project("j", "p", [("left.amount", "amount"),
                           ("right.customer_id", "customer")]),
        group("p", "g", ["customer"], [
            {"op": "count_star", "as": "n"},
            {"op": "sum", "field": "amount", "as": "total"},
            {"op": "min", "field": "amount", "as": "lo"},
            {"op": "max", "field": "amount", "as": "hi"}]),
        rguard("row_count", "g", eq=3),
        rwrite("insert", "customer_summary", "g", {"field": "customer"},
               {"total": {"field": "total"}, "n": {"field": "n"},
                "lo": {"field": "lo"}, "hi": {"field": "hi"}}),
    ])


# Hand-computed from the seed: (order, attribution) pairs of the inner join.
EXPECTED_INNER = {
    "pairs": [(1, 10), (1, 11), (2, 10), (2, 11), (3, 12), (6, 13)],
    "unmatched_left": [4, 5],          # "none" channel; TEXT "1" != INT 1
    "unmatched_right": [],
    "groups": {                        # customer -> (members as pairs, n, total, lo, hi)
        101: {"pairs": [(1, 10), (2, 10), (3, 12)], "n": 3, "total": 220,
              "lo": 50, "hi": 100},
        102: {"pairs": [(1, 11), (2, 11)], "n": 2, "total": 150,
              "lo": 50, "hi": 100},
        103: {"pairs": [(6, 13)], "n": 1, "total": 30, "lo": 30, "hi": 30},
    },
    "summary": {
        101: {"total": 220, "n": 3, "lo": 50, "hi": 100},
        102: {"total": 150, "n": 2, "lo": 50, "hi": 100},
        103: {"total": 30, "n": 1, "lo": 30, "hi": 30},
    },
}


# --- P2: left join, unmatched details kept, written by original row_id -----
def prog_settle_left():
    return rprog("SETTLE-LEFT", [
        rread("o", "orders", [1, 2, 3, 4, 5, 6], row_id_field="oid"),
        rread("a", "attribution", [10, 12, 13]),   # 1:1 channels here
        join("o", "a", "left", [("channel", "channel")], "j"),
        project("j", "p", [("left.oid", "oid"),
                           ("left.amount", "amount"),
                           ("right.customer_id", "customer")]),
        rwrite("insert", "order_enriched", "p", {"field": "oid"},
               {"customer": {"field": "customer"},
                "amount": {"field": "amount"}}),
    ])


EXPECTED_LEFT = {
    "pairs": [(1, 10), (2, 10), (3, 12), (6, 13)],
    "unmatched_left": [4, 5],
    "unmatched_right": [],
    "enriched": {
        1: {"customer": 101, "amount": 100},
        2: {"customer": 101, "amount": 50},
        3: {"customer": 101, "amount": 70},
        4: {"customer": None, "amount": 9},
        5: {"customer": None, "amount": 20},
        6: {"customer": 103, "amount": 30},
    },
}


# --- P3: composite keys, null/missing components never match ---------------
def prog_settle_composite():
    return rprog("SETTLE-COMPOSITE", [
        rread("s", "shipments", [1, 2, 3, 4], row_id_field="sid"),
        rread("l", "lane_owner", [20, 21, 22]),
        join("s", "l", "left", [("lane", "lane"), ("region", "region")], "j"),
        project("j", "p", [("left.sid", "sid"),
                           ("left.kg", "kg"),
                           ("right.owner", "owner")]),
        rwrite("insert", "shipment_enriched", "p", {"field": "sid"},
               {"owner": {"field": "owner"}, "kg": {"field": "kg"}}),
    ])


EXPECTED_COMPOSITE = {
    # shipment 2 (region null) and 3 (region missing) match nothing; lane
    # owner 21 (region null) matches nothing either -- null != null in joins.
    "pairs": [(1, 20), (4, 22)],
    "unmatched_left": [2, 3],
    "unmatched_right": [21],
    "enriched": {
        1: {"owner": "ops", "kg": 40},
        2: {"owner": None, "kg": 41},
        3: {"owner": None, "kg": 42},
        4: {"owner": "air-ops", "kg": 10},
    },
}


# --- P4: join -> join -> group (composite group key) -----------------------
def prog_settle_chain():
    return rprog("SETTLE-CHAIN", [
        rread("o", "orders", [1, 2, 3, 6], row_id_field="oid"),
        rread("a", "attribution", [10, 11, 12, 13]),
        join("o", "a", "inner", [("channel", "channel")], "j1"),
        project("j1", "p1", [("right.customer_id", "customer"),
                             ("left.amount", "amount")]),
        rread("c", "customers", [101, 102, 103], row_id_field="cid"),
        join("p1", "c", "inner", [("customer", "cid")], "j2"),
        project("j2", "p2", [("left.customer", "customer"),
                             ("right.name", "name"),
                             ("left.amount", "amount")]),
        group("p2", "g", ["customer", "name"], [
            {"op": "count_star", "as": "n"},
            {"op": "sum", "field": "amount", "as": "total"}]),
        rwrite("insert", "customer_stats", "g", {"field": "customer"},
               {"name": {"field": "name"}, "total": {"field": "total"},
                "n": {"field": "n"}}),
    ])


EXPECTED_CHAIN = {
    "j1_pairs": [(1, 10), (1, 11), (2, 10), (2, 11), (3, 12), (6, 13)],
    "groups": {  # (customer, name) -> (n, total)
        (101, "acme"): {"n": 3, "total": 220},
        (102, "beta"): {"n": 2, "total": 150},
        (103, "cora"): {"n": 1, "total": 30},
    },
    "stats": {
        101: {"name": "acme", "total": 220, "n": 3},
        102: {"name": "beta", "total": 150, "n": 2},
        103: {"name": "cora", "total": 30, "n": 1},
    },
}


# --- P5: empty input, global aggregation still yields one row --------------
def prog_settle_empty():
    return rprog("SETTLE-EMPTY", [
        rread("e", "orders", [999]),          # absent: empty relation
        group("e", "g", [], [
            {"op": "count_star", "as": "n"},
            {"op": "count", "field": "amount", "as": "na"},
            {"op": "sum", "field": "amount", "as": "total"},
            {"op": "min", "field": "amount", "as": "lo"},
            {"op": "max", "field": "amount", "as": "hi"}]),
        rguard("row_count", "g", eq=1),       # global aggregate: one row
        group("e", "gk", ["channel"], [{"op": "count_star", "as": "n"}]),
        rguard("row_count", "gk", eq=0),      # keyed: no groups at all
        rwrite("insert", "stats", "g", {"const": 1},
               {"n": {"field": "n"}, "na": {"field": "na"},
                "total": {"field": "total"}, "lo": {"field": "lo"},
                "hi": {"field": "hi"}}),
    ])


EXPECTED_EMPTY = {"stats": {1: {"n": 0, "na": 0, "total": None,
                                "lo": None, "hi": None}}}


# --- P6: exact cancellation, final result representable --------------------
def prog_settle_cancel():
    return rprog("SETTLE-CANCEL", [
        rread("e", "adjustments", [1, 2, 3]),
        group("e", "g", [], [{"op": "sum", "field": "amt", "as": "total"}]),
        rwrite("insert", "stats", "g", {"const": 2},
               {"total": {"field": "total"}}),
    ])


EXPECTED_CANCEL = {"total": INT62}     # 2**62 + 2**62 - 2**62 == 2**62


# --- P7: final sum overflows int64 -> whole batch rejected -----------------
def prog_settle_overflow():
    return rprog("SETTLE-OVERFLOW", [
        rread("e", "overflows", [1, 2]),
        group("e", "g", [], [{"op": "sum", "field": "amt", "as": "total"}]),
        rwrite("insert", "stats", "g", {"const": 3},
               {"total": {"field": "total"}}),
    ])


# --- P8: two derived rows compute the same target row_id -> rejected -------
def prog_settle_collide():
    return rprog("SETTLE-COLLIDE", [
        rread("o", "orders", [1, 3]),
        group("o", "g", ["channel"], [{"op": "count_star", "as": "n"}]),
        # Both groups ("web", "app") compute target 700 with identical
        # content; the collision must still reject the whole batch.
        rwrite("insert", "stats", "g", {"const": 700},
               {"x": {"const": 1}}),
    ])


# --- P9: aggregate over a text field -> rejected with located diagnostics --
def prog_settle_typeerr():
    return rprog("SETTLE-TYPEERR", [
        rread("o", "orders", [1, 2]),
        group("o", "g", [], [{"op": "sum", "field": "channel", "as": "total"}]),
        rwrite("insert", "stats", "g", {"const": 4},
               {"total": {"field": "total"}}),
    ])


# --- P10: terminal constraint violation rejects the whole relational batch -
SETTLEMENT_RULES = {
    "schema": "managed-constraints/v1",
    "rules": [
        {"name": "audit_customer_fk", "kind": "foreign_key",
         "child": "audit_summary", "field": "customer_ref",
         "parent": "customers", "on_delete": "RESTRICT"},
        {"name": "enriched_ticket_u", "kind": "unique",
         "table": "order_enriched", "fields": ["ticket"]},
    ],
}


def prog_settle_fk_violation():
    return rprog("SETTLE-FK", [
        rread("o", "orders", [1, 2, 3, 4, 5, 6], row_id_field="oid"),
        rread("a", "attribution", [10, 11, 12, 13]),
        join("o", "a", "inner", [("channel", "channel")], "j"),
        project("j", "p", [("right.customer_id", "customer"),
                           ("left.amount", "amount")]),
        group("p", "g", ["customer"], [
            {"op": "count_star", "as": "n"},
            {"op": "sum", "field": "amount", "as": "total"}]),
        # customer_ref = customer + 1000 -> dangling parent on purpose.
        rwrite("insert", "audit_summary", "g", {"field": "customer"},
               {"customer_ref": {"add": [{"field": "customer"},
                                         {"const": 1000}]},
                "total": {"field": "total"}}),
    ])


def prog_settle_unique_violation():
    """Two derived rows share one unique ticket value -> rejected at batch end."""
    return rprog("SETTLE-UNIQUE", [
        rread("o", "orders", [1, 2], row_id_field="oid"),
        project("o", "p", [("oid", "oid"), ("amount", "amount")]),
        rwrite("insert", "order_enriched", "p", {"field": "oid"},
               {"ticket": {"const": "T-1"}, "amount": {"field": "amount"}}),
    ])


# ---------------------------------------------------------------------------
# Revocation drill: revoking the batch that deleted an attribution row
# restores a right-side row; the downstream settlement is re-derived (not
# patched), a later guard blocks, and an unrelated v1 partition batch stays
# skipped.
# ---------------------------------------------------------------------------
def revocation_seed():
    return {
        "orders": {
            1: {"channel": "web", "amount": 100},
            2: {"channel": "web", "amount": 50},
            3: {"channel": "app", "amount": 70},
        },
        "attribution": {
            10: {"channel": "web", "customer_id": 101},
            11: {"channel": "app", "customer_id": 102},
        },
        "customer_summary": {},
        "order_enriched": {},
        "stats": {},
        "gamma": {100: {"value": 3}},
    }


def revocation_batches():
    target = v1prog("T", [
        v1read("a", "attribution", [11]),
        {"op": "write", "effect": "delete", "table": "attribution",
         "rows_alias": "a",
         "rows": [{"source_row_id": 11, "row_id": {"const": 11}}]},
    ])
    proc = rprog("PROC", [
        rread("o", "orders", [1, 2, 3], row_id_field="oid"),
        rread("a", "attribution", [10, 11]),
        join("o", "a", "inner", [("channel", "channel")], "ji"),
        project("ji", "pi", [("right.customer_id", "customer"),
                             ("left.amount", "amount")]),
        group("pi", "g", ["customer"], [
            {"op": "count_star", "as": "n"},
            {"op": "sum", "field": "amount", "as": "total"}]),
        rwrite("insert", "customer_summary", "g", {"field": "customer"},
               {"total": {"field": "total"}, "n": {"field": "n"}}),
        join("o", "a", "left", [("channel", "channel")], "jl"),
        project("jl", "pl", [("left.oid", "oid"),
                             ("right.customer_id", "customer"),
                             ("left.amount", "amount")]),
        rwrite("insert", "order_enriched", "pl", {"field": "oid"},
               {"customer": {"field": "customer"},
                "amount": {"field": "amount"}}),
    ])
    guard = rprog("G", [
        rread("s", "customer_summary", [101, 102]),
        rguard("int_sum", "s", field="total", le=150),
        group("s", "gs", [], [{"op": "sum", "field": "total", "as": "grand"}]),
        rwrite("insert", "stats", "gs", {"const": 1},
               {"grand": {"field": "grand"}}),
    ])
    indep = v1prog("Z", [
        v1read("g", "gamma", [100]),
        {"op": "write", "effect": "replace", "table": "gamma",
         "rows_alias": "g",
         "rows": [{"source_row_id": 100, "row_id": {"const": 100},
                   "values": {"value": {"const": 7}}}]},
    ], partition={"table": "gamma", "ranges": [[100, 199]]})
    return [target, proc, guard, indep]


# Hand-computed states for the revocation drill.
EXPECTED_REV_ORIGINAL = {
    "proc_inner_pairs": [(1, 10), (2, 10)],
    "proc_left_pairs": [(1, 10), (2, 10)],
    "proc_left_unmatched": [3],
    "customer_summary": {101: {"total": 150, "n": 2}},
    "order_enriched": {1: {"customer": 101, "amount": 100},
                       2: {"customer": 101, "amount": 50},
                       3: {"customer": None, "amount": 70}},
    "stats": {1: {"grand": 150}},
}
EXPECTED_REV_AFTER = {
    "proc_inner_pairs": [(1, 10), (2, 10), (3, 11)],
    "proc_left_pairs": [(1, 10), (2, 10), (3, 11)],
    "proc_left_unmatched": [],
    "customer_summary": {101: {"total": 150, "n": 2},
                         102: {"total": 70, "n": 1}},
    "order_enriched": {1: {"customer": 101, "amount": 100},
                       2: {"customer": 101, "amount": 50},
                       3: {"customer": 102, "amount": 70}},
    "stats": {},          # G blocked: its write never happens in the new epoch
    "gamma": {100: {"value": 7}},
}
