"""Fixtures for the offline CDC transaction ingestor.

The module contains:

* ``CdcConfig`` builders and a small set of fragment files' worth of
  transactions for the main drill (a later transaction is delivered first;
  a child row is inserted before its parent; a second transaction performs two
  consecutive updates);
* :class:`MiniModel` -- an *independent* dictionary model that re-derives the
  expected managed rows, row_id mappings and cursor purely from the source
  transactions and their delivery order.  It imports nothing from the product
  package, so drill assertions compare the product against an independently
  hand-maintained model rather than against its own bookkeeping.
"""

import copy

from revoke_ledger.cdcp import (CDC_CONFIG_SCHEMA, CDC_FRAGMENT_SCHEMA,
                                INT64_MAX)

SRC_CUSTOMER = "customer"
SRC_ORDER = "order"
TBL_CUSTOMER = "dim_customer"
TBL_ORDER = "fact_order"
SOURCE_ID = "crm"


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------
def source_config(source_id=SOURCE_ID, start_seq=0):
    return {
        "schema": CDC_CONFIG_SCHEMA,
        "source_id": source_id,
        "start_seq": start_seq,
        "tables": [
            {"source_table": SRC_CUSTOMER, "managed_table": TBL_CUSTOMER,
             "key_fields": ["tenant", "code"], "references": []},
            {"source_table": SRC_ORDER, "managed_table": TBL_ORDER,
             "key_fields": ["oid"],
             "references": [{"column": "cust", "references": SRC_CUSTOMER}]},
        ],
    }


def constrained_config(source_id="crm_c", start_seq=0):
    """Config whose tables match the drill's declarative constraint rules."""
    cfg = source_config(source_id, start_seq)
    return cfg


def constraint_rules():
    """Foreign key on the converted reference column plus a unique rule."""
    return {
        "schema": "managed-constraints/v1",
        "rules": [
            {"name": "order_customer_fk", "kind": "foreign_key",
             "child": TBL_ORDER, "field": "cust",
             "parent": TBL_CUSTOMER, "on_delete": "RESTRICT"},
            {"name": "customer_bizkey_u", "kind": "unique",
             "table": TBL_CUSTOMER, "fields": ["tenant", "code"]},
        ],
    }


# ---------------------------------------------------------------------------
# fragment construction
# ---------------------------------------------------------------------------
def fragment(source_id, tx_seq, tx_id, part, **extra):
    out = {"schema": CDC_FRAGMENT_SCHEMA, "source_id": source_id,
           "tx_seq": tx_seq, "tx_id": tx_id, "part": part}
    out.update(extra)
    return out


def begin(source_id, tx_seq, tx_id):
    return fragment(source_id, tx_seq, tx_id, "begin")


def end(source_id, tx_seq, tx_id, change_count, outcome="commit"):
    return fragment(source_id, tx_seq, tx_id, "end",
                    change_count=change_count, outcome=outcome)


def change(source_id, tx_seq, tx_id, index, table, op, before, after):
    return fragment(source_id, tx_seq, tx_id, "change", index=index,
                    table=table, op=op, before=before, after=after)


def customer_key(tenant, code):
    return [tenant, code]


# ---------------------------------------------------------------------------
# main-drill transactions (hand authored)
# ---------------------------------------------------------------------------
def drill_transactions(source_id=SOURCE_ID):
    """Return {tx_seq: (tx_id, [fragments])} for the four-transaction drill.

    tx 0: insert child (order) BEFORE parent (customer) in fragment order.
    tx 1: two consecutive updates of the same customer (repeated in-tx update
          plus a delete/recreate of an order).
    tx 2: abort (fully assembled, row effects ignored).
    tx 3: delivered first in the main drill; cannot advance until 0..2 settle.
    """
    txs = {}

    # ---- tx 0: child first, parent second --------------------------------
    c0 = {"tenant": "a", "code": "1", "name": "ada"}
    o0 = {"oid": 7, "cust": customer_key("a", "1"), "amt": 100}
    txs[0] = ("T0", [
        begin(source_id, 0, "T0"),
        change(source_id, 0, "T0", 2, SRC_ORDER, "insert", None,
               copy.deepcopy(o0)),
        change(source_id, 0, "T0", 1, SRC_CUSTOMER, "insert", None,
               copy.deepcopy(c0)),
        end(source_id, 0, "T0", 2),
    ])

    # ---- tx 1: repeated update + delete/recreate --------------------------
    c1a = {"tenant": "a", "code": "1", "name": "ada v2"}
    c1b = {"tenant": "a", "code": "1", "name": "ada v3"}
    o_before = copy.deepcopy(o0)
    o_mid = {"oid": 7, "cust": customer_key("a", "1"), "amt": 140}
    txs[1] = ("T1", [
        begin(source_id, 1, "T1"),
        change(source_id, 1, "T1", 1, SRC_CUSTOMER, "update",
               copy.deepcopy(c0), copy.deepcopy(c1a)),
        change(source_id, 1, "T1", 2, SRC_ORDER, "update",
               copy.deepcopy(o_before), copy.deepcopy(o_mid)),
        change(source_id, 1, "T1", 3, SRC_CUSTOMER, "update",
               copy.deepcopy(c1a), copy.deepcopy(c1b)),
        change(source_id, 1, "T1", 4, SRC_ORDER, "delete",
               copy.deepcopy(o_mid), None),
        change(source_id, 1, "T1", 5, SRC_ORDER, "insert", None,
               copy.deepcopy(o_mid)),
        end(source_id, 1, "T1", 5),
    ])

    # ---- tx 2: complete abort --------------------------------------------
    txs[2] = ("T2", [
        begin(source_id, 2, "T2"),
        change(source_id, 2, "T2", 1, SRC_CUSTOMER, "delete",
               copy.deepcopy(c1b), None),
        end(source_id, 2, "T2", 1, outcome="abort"),
    ])

    # ---- tx 3: later, delivered first ------------------------------------
    c3 = {"tenant": "b", "code": "9", "name": "ben"}
    txs[3] = ("T3", [
        begin(source_id, 3, "T3"),
        change(source_id, 3, "T3", 1, SRC_CUSTOMER, "insert", None,
               copy.deepcopy(c3)),
        end(source_id, 3, "T3", 1),
    ])
    return txs


def drill_delivery_order():
    """Fragment delivery batches for the main drill (later tx first)."""
    txs = drill_transactions()
    return [
        ("later-tx-first", txs[3][1]),
        ("tx0-shuffled", [txs[0][1][3], txs[0][1][0], txs[0][1][2],
                          txs[0][1][1]]),       # end, begin, change2, change1
        ("tx1", txs[1][1]),
        ("tx2-abort", txs[2][1]),
    ]


# ---------------------------------------------------------------------------
# independent dictionary model (no product imports beyond constants)
# ---------------------------------------------------------------------------
class MiniModel:
    """Hand-maintained expected state, driven by source transactions.

    Manages its own row_id allocator per managed table (monotonic high water,
    deterministic over the key *set* required by each transaction, so arrival
    order of fragments cannot change a mapping), retains mappings after
    deletes, converts reference key arrays to row ids and applies changes in
    index order with full, type-sensitive before-image checks.
    """

    def __init__(self, config):
        self.config = config
        self.source_id = config["source_id"]
        self.tables = {tc["source_table"]: tc for tc in config["tables"]}
        self.managed_of = {tc["source_table"]: tc["managed_table"]
                           for tc in config["tables"]}
        # (source_table, tuple(key components)) -> row_id
        self.identity = {}
        self.high_water = {tc["managed_table"]: 0 for tc in config["tables"]}
        # managed rows: {managed_table: {row_id: row}}
        self.state = {tc["managed_table"]: {} for tc in config["tables"]}
        self.next_seq = config["start_seq"]
        self.decisions = {}

    @staticmethod
    def _key(row, key_fields):
        # Typed tuple: int 1 / bool True / text "1" remain distinct.
        return tuple(row[f] for f in key_fields)

    def _allocate(self, source_table, key):
        ident = (source_table, key)
        if ident in self.identity:
            return self.identity[ident]
        mng = self.managed_of[source_table]
        self.high_water[mng] += 1
        rid = self.high_water[mng]
        if rid > INT64_MAX:
            raise OverflowError("row_id capacity exhausted")
        self.identity[ident] = rid
        return rid

    def _convert(self, row, tc):
        refs = {r["column"]: r["references"] for r in tc["references"]}
        out = {}
        for col, val in row.items():
            if col in refs:
                if val is None:
                    out[col] = None
                else:
                    target_tc = self.tables[refs[col]]
                    key = tuple(val)
                    out[col] = self._allocate(target_tc["source_table"], key)
            else:
                out[col] = copy.deepcopy(val)
        return out

    def expected_rows_after(self, tx_seqs):
        """Pure replay of the given committed source transactions -> rows.

        Returns (state snapshot, identity map, high water).  Allocations are
        derived per transaction from the sorted required key set, so the same
        initial state and the same transactions yield identical mappings no
        matter in what order their fragments arrived.
        """
        model = MiniModel(self.config)
        all_txs = drill_transactions(self.source_id)
        for seq in tx_seqs:
            tx_id, frags = all_txs[seq]
            end_frag = next(f for f in frags if f["part"] == "end")
            if end_frag["outcome"] == "abort":
                model.decisions[seq] = "aborted"
                model.next_seq = seq + 1
                continue
            changes = sorted((f for f in frags if f["part"] == "change"),
                             key=lambda f: f["index"])
            # Phase 1: pre-register every key the transaction will need, in a
            # deterministic order, before touching any row.
            needed = set()
            for ch in changes:
                tc = model.tables[ch["table"]]
                row = ch["after"] if ch["after"] is not None else ch["before"]
                needed.add((ch["table"], model._key(row, tc["key_fields"])))
                for ref in tc["references"]:
                    src_row = ch["after"] if ch["after"] is not None \
                        else ch["before"]
                    if src_row.get(ref["column"]) is not None:
                        needed.add((ref["references"],
                                    tuple(src_row[ref["column"]])))
            for source_table, key in sorted(needed):
                model._allocate(source_table, key)

            # Phase 2: apply changes in index order with full before checks.
            for ch in changes:
                tc = model.tables[ch["table"]]
                mng = model.managed_of[ch["table"]]
                row_src = ch["after"] if ch["after"] is not None \
                    else ch["before"]
                rid = model._allocate(
                    ch["table"], model._key(row_src, tc["key_fields"]))
                current = model.state[mng].get(rid)
                want_before = None if ch["before"] is None else \
                    model._convert(ch["before"], tc)
                if want_before is None:
                    assert current is None, \
                        f"tx{seq} change {ch['index']}: row should be absent"
                else:
                    assert _typed_equal(current, want_before), \
                        f"tx{seq} change {ch['index']}: before mismatch " \
                        f"{current} != {want_before}"
                if ch["op"] == "insert":
                    model.state[mng][rid] = model._convert(ch["after"], tc)
                elif ch["op"] == "delete":
                    del model.state[mng][rid]
                else:
                    model.state[mng][rid] = model._convert(ch["after"], tc)
            model.decisions[seq] = "committed"
            model.next_seq = seq + 1
        return (copy.deepcopy(model.state),
                {k: v for k, v in model.identity.items()},
                dict(model.high_water), model.next_seq)


def _typed_equal(a, b):
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return set(a) == set(b) and all(_typed_equal(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(
            _typed_equal(x, y) for x, y in zip(a, b))
    return a == b


# Hand-computed expectations after each prefix of the main drill.
def expected_after_tx0():
    state, identity, water, next_seq = MiniModel(
        source_config()).expected_rows_after([0])
    return state, identity, water, next_seq


def expected_after_tx01():
    state, identity, water, next_seq = MiniModel(
        source_config()).expected_rows_after([0, 1])
    return state, identity, water, next_seq


def expected_after_tx012():
    # tx 2 aborts: state identical to after tx 1.
    state, identity, water, next_seq = MiniModel(
        source_config()).expected_rows_after([0, 1, 2])
    return state, identity, water, next_seq


def expected_final():
    state, identity, water, next_seq = MiniModel(
        source_config()).expected_rows_after([0, 1, 2, 3])
    return state, identity, water, next_seq
