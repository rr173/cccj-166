"""Fixtures: the initial state and the drill batches in program form.

Programs are plain JSON-able data; nothing here executes except through the
validated interpreter.  The drill catalog is keyed by scenario name.
"""

import copy

INITIAL_ROWS = {
    "alpha": {
        1: {"amount": 10},
        2: {"amount": 5},
    },
    "gamma": {
        100: {"value": 3},
    },
}


def _read(alias, table, ids, where):
    return {"op": "read", "alias": alias,
            "select": {"table": table, "where": where},
            "map": [{"row_id": i, "table": table} for i in ids]}


def _write(effect, table, alias, rows):
    return {"op": "write", "effect": effect, "table": table,
            "rows_alias": alias, "rows": rows}


def _ins(src, target, **values):
    return {"source_row_id": src, "row_id": {"const": target},
            "values": {k: {"const": v} for k, v in values.items()}}


def _copy(src, target, cols):
    return {"source_row_id": src, "row_id": {"const": target},
            "values": {c: {"field": c} for c in cols}}


def prog(lid, steps, partition=None):
    p = {"logical_batch_id": lid, "schema": "managed-batch/v1", "steps": steps}
    if partition is not None:
        p["partition"] = partition
    return p


# The main drill (business scenario in the task statement).
def main_drill():
    initial = copy.deepcopy(INITIAL_ROWS)
    initial["beta"] = {}
    batches = [
        # A: set alpha[1].amount = 10 (already 10; a deterministic overwrite)
        prog("A", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"const": 10}}}]),
        ]),
        # target: alpha[1].amount := 20 (nudge by +10 from the read field)
        prog("T", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"add": [{"field": "amount"}, {"const": 10}]}}}]),
        ]),
        # B: beta[500].value := alpha[1].amount * 2, computed as x+x (exact ints)
        prog("B", [
            _read("a", "alpha", [1], {"kind": "all"}),
            _write("replace", "beta", "a", [
                {"source_row_id": 1, "row_id": {"const": 500},
                 "values": {"value": {"add": [{"field": "a.amount"},
                                               {"field": "a.amount"}]}}}]),
        ]),
        # C: independent partition gamma, value 7
        prog("C", [
            _read("g", "gamma", [100], {"kind": "all"}),
            _write("replace", "gamma", "g", [
                {"source_row_id": 100, "row_id": {"const": 100},
                 "values": {"value": {"const": 7}}}]),
        ], partition={"table": "gamma", "ranges": [[100, 199]]}),
    ]
    return initial, batches


def guard_drill():
    """Same as main drill plus a later batch whose guard is A>=15."""
    initial, batches = main_drill()
    batches = copy.deepcopy(batches)
    batches.append(prog("G", [
        _read("r", "alpha", [1], {"kind": "all"}),
        {"op": "guard", "test": "row_count", "alias": "r", "eq": 1},
        {"op": "guard", "test": "int_sum", "alias": "r", "field": "amount", "ge": 15},
        _write("replace", "alpha", "r", [
            {"source_row_id": 1, "row_id": {"const": 1},
             "values": {"amount": {"field": "amount"}}}]),
    ]))
    return initial, batches


def empty_guard_drill():
    """Later batch's guard fires only if a previously-empty read becomes non-empty."""
    initial = {
        "alpha": {1: {"amount": 10}},
        "probe": {},
        "beta": {},
    }
    batches = [
        prog("T", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"const": 20}}}]),
        ]),
        # E: guard on an empty read; if empty it writes nothing and fails.
        prog("E", [
            _read("p", "probe", [9], {"kind": "all"}),
            {"op": "guard", "test": "row_count", "alias": "p", "eq": 1},
            _write("replace", "beta", "p", [
                {"source_row_id": 9, "row_id": {"const": 900},
                 "values": {"value": {"const": 1}}}]),
        ]),
        # Z: inserts probe[9] only if alpha[1].amount >= 20 (i.e. only in the
        # original timeline, not after revocation).
        prog("Z", [
            _read("a", "alpha", [1], {"kind": "all"}),
            {"op": "guard", "test": "int_sum", "alias": "a", "field": "amount", "ge": 20},
            _write("replace", "probe", "a", [
                {"source_row_id": 1, "row_id": {"const": 9},
                 "values": {"amount": {"const": 1}}}]),
        ]),
    ]
    return initial, batches


def predicate_range_drill():
    """Revocation makes a row enter a selection window that previously excluded it."""
    initial = {
        "alpha": {1: {"amount": 10}, 2: {"amount": 30}},
        "beta": {},
    }
    batches = [
        # target sets alpha[1] from 10 to 40
        prog("T", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"const": 40}}}]),
        ]),
        # W reads the 20..35 window: originally selects alpha[2] only.  After
        # revocation alpha[1]=10 so window stays {2}; but this test uses
        # 10..35 to show alpha[1] entering the selection.
        prog("W", [
            _read("r", "alpha", [1, 2], {"kind": "range", "field": "amount",
                                         "lo": 10, "hi": 35}),
            {"op": "guard", "test": "row_count", "alias": "r", "eq": 1},
            _write("replace", "beta", "r", [
                {"source_row_id": 2, "row_id": {"const": 200},
                 "values": {"value": {"field": "amount"}}}]),
        ]),
    ]
    return initial, batches


def overwrite_same_id_drill():
    """A later batch replaces the same row_id the target wrote, from a read."""
    initial = {"alpha": {1: {"amount": 10}}, "beta": {}}
    batches = [
        prog("T", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"const": 20}}}]),
        ]),
        # B reads alpha[1] and writes beta[500]; it overwrites alpha[1] with
        # +1, i.e. the same row_id the target wrote earlier.
        prog("B", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("insert", "beta", "r", [
                {"source_row_id": 1, "row_id": {"const": 500},
                 "values": {"value": {"field": "amount"}}}]),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"add": [{"field": "amount"},
                                               {"const": 1}]}}}]),
        ]),
    ]
    return initial, batches


def multi_step_failure_drill():
    """A later batch writes, then fails a later step; the whole batch aborts.

    Busy little table: beta[500] is written by the first write step, then the
    second step tries an insert at the already-written id -> primary-key
    conflict -> the batch must leave *no* writes behind.  On the original run
    this makes the batch a recorded failure; on revocation replay the pattern is
    re-evaluated from the new input.
    """
    initial = {"alpha": {1: {"amount": 10}}, "beta": {}}
    batches = [
        prog("T", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"const": 20}}}]),
        ]),
        prog("M", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("insert", "beta", "r", [
                {"source_row_id": 1, "row_id": {"const": 500},
                 "values": {"value": {"field": "amount"}}}]),
            _write("insert", "beta", "r", [
                {"source_row_id": 1, "row_id": {"const": 500},
                 "values": {"value": {"const": 0}}}]),
        ]),
        # A later successful batch that also touches beta, so blocked-vs-not is
        # visible in the final state.
        prog("N", [
            _read("r", "beta", [500], {"kind": "all"}),
            _write("replace", "beta", "r", [
                {"source_row_id": 500, "row_id": {"const": 500},
                 "values": {"value": {"field": "value"}}}]),
        ]),
    ]
    return initial, batches


def multi_step_failure_success_drill():
    """A batch that succeeds originally but fails its second step when the
    first step's guard input changes, so revocation blocks it with no writes."""
    initial = {"alpha": {1: {"amount": 10}}, "beta": {}}
    batches = [
        prog("T", [
            _read("r", "alpha", [1], {"kind": "all"}),
            _write("replace", "alpha", "r", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"amount": {"const": 20}}}]),
        ]),
        # M inserts beta[500] then beta[501] only while alpha amount >= 20;
        # after revocation amount is 10, the guard blocks before any write.
        prog("M", [
            _read("r", "alpha", [1], {"kind": "all"}),
            {"op": "guard", "test": "int_sum", "alias": "r", "field": "amount",
             "ge": 20},
            _write("insert", "beta", "r", [
                {"source_row_id": 1, "row_id": {"const": 500},
                 "values": {"value": {"field": "amount"}}}]),
            _write("insert", "beta", "r", [
                {"source_row_id": 1, "row_id": {"const": 501},
                 "values": {"value": {"const": 1}}}]),
        ]),
    ]
    return initial, batches


# ---------------------------------------------------------------------------
# Constraint drills.  Every fixture is deliberately small so that the expected
# deletion set and final row set can be enumerated by hand (and re-derived by
# the independent constraint_oracle, not by the product closure).
# ---------------------------------------------------------------------------
CONSTRAINT_RULES = {
    "schema": "managed-constraints/v1",
    "rules": [
        {"name": "owner_code_u", "kind": "unique", "table": "owner",
         "fields": ["code"]},
        {"name": "account_owner_fk", "kind": "foreign_key", "child": "account",
         "field": "owner_id", "parent": "owner", "on_delete": "CASCADE"},
        {"name": "ledger_account_fk", "kind": "foreign_key", "child": "entry",
         "field": "account_id", "parent": "account", "on_delete": "CASCADE"},
        {"name": "entry_pair_u", "kind": "unique", "table": "entry",
         "fields": ["account_id", "kind"]},
        {"name": "note_owner_fk", "kind": "foreign_key", "child": "note",
         "field": "owner_id", "parent": "owner", "on_delete": "RESTRICT"},
    ],
}


def constraint_drill():
    """Children before parents, then a two-path cascade through a cycle.

    owner 1 "acme", owner 2 "beta"; account 10 -> owner 1, 11 -> owner 1,
    12 -> owner 2; entry 100 -> account 10, 101 -> account 11.

    Batch P inserts account[20] (owner 1) and its entry[200] *before* the
    parent owner[3] "acme-2" exists in the same batch: legal because the check
    is on the completed batch.  Batch X swaps two unique codes in one batch.
    """
    initial = {
        "owner": {1: {"code": "acme"}, 2: {"code": "beta"}},
        "account": {10: {"owner_id": 1}, 11: {"owner_id": 1},
                    12: {"owner_id": 2}},
        "entry": {100: {"account_id": 10, "kind": "debit"},
                  101: {"account_id": 11, "kind": "debit"}},
        "note": {},
        "scratch": {1000: {"z": 0}, 1001: {"z": 0}, 1002: {"z": 0},
                    1003: {"z": 0}},
    }
    batches = [
        # B1: insert the child entry[200] pointing at account[20], and the
        # account[20] pointing at owner[3], all in one batch, parents last.
        prog("B1", [
            _read("s", "scratch", [1000], {"kind": "all"}),
            _write("insert", "entry", "s", [
                {"source_row_id": 1000, "row_id": {"const": 200},
                 "values": {"account_id": {"const": 20},
                            "kind": {"const": "debit"}}}]),
            _write("insert", "account", "s", [
                {"source_row_id": 1000, "row_id": {"const": 20},
                 "values": {"owner_id": {"const": 3}}}]),
            _write("insert", "owner", "s", [
                {"source_row_id": 1000, "row_id": {"const": 3},
                 "values": {"code": {"const": "acme-2"}}}]),
        ]),
        # B2: swap owner[1].code and owner[2].code in one batch (two replaces
        # whose intermediate states would each collide).
        prog("B2", [
            _read("s", "scratch", [1001], {"kind": "all"}),
            _write("replace", "owner", "s", [
                {"source_row_id": 1001, "row_id": {"const": 1},
                 "values": {"code": {"const": "beta"}}}]),
            _write("replace", "owner", "s", [
                {"source_row_id": 1001, "row_id": {"const": 2},
                 "values": {"code": {"const": "acme"}}}]),
        ]),
        # B3: the target.  Deletes owner[1]; the cascade must reach
        # account[10], account[11] -> entry[100], entry[101].  note[?] would
        # block, but the drill keeps notes empty.
        prog("B3", [
            _read("s", "scratch", [1002], {"kind": "all"}),
            _write("delete", "owner", "s", [
                {"source_row_id": 1002, "row_id": {"const": 1}}]),
        ]),
    ]
    return initial, batches


def constraint_swap_failure_drill():
    """Final duplicate after a swap: the whole batch must be rejected."""
    initial = {
        "owner": {1: {"code": "a"}, 2: {"code": "b"}, 3: {"code": "c"}},
        "account": {}, "entry": {}, "note": {},
        "scratch": {1: {"z": 0}},
    }
    batches = [
        # T sets owner[1].code = owner[3].code's value ("c"): a* final duplicate.
        prog("T", [
            _read("s", "scratch", [1], {"kind": "all"}),
            _write("replace", "owner", "s", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"code": {"const": "c"}}}]),
        ]),
    ]
    return initial, batches


CYCLE_RULES = {
    "schema": "managed-constraints/v1",
    "rules": [
        {"name": "node_owner_fk", "kind": "foreign_key", "child": "node",
         "field": "owner_id", "parent": "owner", "on_delete": "CASCADE"},
        {"name": "node_parent_fk", "kind": "foreign_key", "child": "node",
         "field": "parent_id", "parent": "node", "on_delete": "CASCADE"},
    ],
}


def constraint_cycle_drill():
    """One deletion reaches the same row by two paths and crosses a cycle.

    owner[1]
      -> node[1]  (owner_id = 1)
           -> node[2] (parent_id = 1)
                -> node[3] (parent_id = 2)
                -> node[9] (parent_id = 2)
      -> node[7]  (owner_id = 1)
           -> node[8] (parent_id = 7)
           -> node[7] (parent_id = 8)   <-- cycle
      -> node[9]  (owner_id = 1)        <-- second path to 9

    Expected deletion set: {1,2,3,7,8,9} plus the root owner[1].  The cycle
    must not loop and node[9] must be deleted exactly once.
    """
    initial = {
        "owner": {1: {"code": "root"}, 2: {"code": "keep"}},
        "node": {1: {"owner_id": 1, "parent_id": None},
                 2: {"owner_id": None, "parent_id": 1},
                 3: {"owner_id": None, "parent_id": 2},
                 7: {"owner_id": 1, "parent_id": 8},
                 8: {"owner_id": None, "parent_id": 7},
                 9: {"owner_id": 1, "parent_id": 2}},
        "scratch": {1: {"z": 0}},
    }
    batches = [
        prog("T", [
            _read("s", "scratch", [1], {"kind": "all"}),
            _write("delete", "owner", "s", [
                {"source_row_id": 1, "row_id": {"const": 1}}]),
        ]),
    ]
    return initial, batches


RESTRICT_RULES = {
    "schema": "managed-constraints/v1",
    "rules": [
        {"name": "account_owner_fk", "kind": "foreign_key", "child": "account",
         "field": "owner_id", "parent": "owner", "on_delete": "CASCADE"},
        {"name": "note_owner_fk", "kind": "foreign_key", "child": "note",
         "field": "owner_id", "parent": "owner", "on_delete": "RESTRICT"},
        {"name": "note_ledger_fk", "kind": "foreign_key", "child": "note",
         "field": "ledger_id", "parent": "ledger", "on_delete": "CASCADE"},
    ],
}


def constraint_restrict_drill(also_delete_ledger):
    """A RESTRICT child swept away by an independent cascade path.

    note[900] references owner[1] under RESTRICT and ledger[50] under CASCADE.
    The batch always deletes owner[1].  When `also_delete_ledger` is True it
    additionally deletes ledger[50]; the cascade takes note[900] *after* the
    explicit steps and before the RESTRICT check, so deleting owner[1] is
    allowed.  When False, note[900] survives and the batch is rejected.
    """
    initial = {
        "owner": {1: {"code": "a"}},
        "account": {10: {"owner_id": 1}},
        "note": {900: {"owner_id": 1, "ledger_id": 50}},
        "ledger": {50: {"label": "L"}},
        "scratch": {1: {"z": 0}},
    }
    steps = [
        _read("s", "scratch", [1], {"kind": "all"}),
        _write("delete", "owner", "s", [
            {"source_row_id": 1, "row_id": {"const": 1}}]),
    ]
    if also_delete_ledger:
        # A second, independent root.  Its cascade reaches the same note row.
        steps.append(_write("delete", "ledger", "s", [
            {"source_row_id": 1, "row_id": {"const": 50}}]))
    return initial, [prog("T", steps)]


def constraint_delete_recreate_drill():
    """Parent deleted and re-created in one batch is NOT a deletion root."""
    initial = {
        "owner": {1: {"code": "a"}},
        "account": {10: {"owner_id": 1}},
        "entry": {}, "note": {}, "scratch": {1: {"z": 0}},
    }
    batches = [
        prog("T", [
            _read("s", "scratch", [1], {"kind": "all"}),
            _write("delete", "owner", "s", [
                {"source_row_id": 1, "row_id": {"const": 1}}]),
            _write("replace", "owner", "s", [
                {"source_row_id": 1, "row_id": {"const": 1},
                 "values": {"code": {"const": "a"}}}]),
        ]),
    ]
    return initial, batches


def constraint_new_child_of_deleted_parent_drill():
    """A child inserted in the same batch that deletes its parent cascades."""
    initial = {
        "owner": {1: {"code": "a"}, 2: {"code": "b"}},
        "account": {}, "entry": {}, "note": {},
        "scratch": {1: {"z": 0}},
    }
    batches = [
        prog("T", [
            _read("s", "scratch", [1], {"kind": "all"}),
            _write("insert", "account", "s", [
                {"source_row_id": 1, "row_id": {"const": 77},
                 "values": {"owner_id": {"const": 1}}}]),
            _write("delete", "owner", "s", [
                {"source_row_id": 1, "row_id": {"const": 1}}]),
        ]),
    ]
    return initial, batches


def constraint_phantom_parent_drill():
    """A child pointing at a parent that never existed is rejected, not cascaded."""
    initial = {
        "owner": {1: {"code": "a"}},
        "account": {}, "entry": {}, "note": {},
        "scratch": {1: {"z": 0}},
    }
    batches = [
        prog("T", [
            _read("s", "scratch", [1], {"kind": "all"}),
            _write("insert", "account", "s", [
                {"source_row_id": 1, "row_id": {"const": 55},
                 "values": {"owner_id": {"const": 404}}}]),
        ]),
    ]
    return initial, batches


def constraint_revocation_drill():
    """Target adds a parent; a later batch (other table) references it.

    Revoking the target must block the later batch for a missing parent, even
    though the later batch declares a partition on `account` (its own read and
    write table) while the target writes `owner` and reads `scratch`.  A truly
    unrelated `note` batch must still be skippable.
    """
    initial = {
        "owner": {9: {"code": "seed-owner"}},
        "account": {1: {"owner_id": 9}},
        "entry": {}, "note": {7: {"owner_id": 9}},
        "scratch": {1: {"z": 0}},
    }
    batches = [
        # T: add owner[5] "target-owned".
        prog("T", [
            _read("s", "scratch", [1], {"kind": "all"}),
            _write("insert", "owner", "s", [
                {"source_row_id": 1, "row_id": {"const": 5},
                 "values": {"code": {"const": "target-owned"}}}]),
        ]),
        # L: reads/writes only `account` (partition [1,2]); inserts account[2]
        # referencing owner[5].  It never reads `owner`, so only the declared
        # constraint dependency exposes the external reference.
        prog("L", [
            _read("a", "account", [1], {"kind": "all"}),
            _write("insert", "account", "a", [
                {"source_row_id": 1, "row_id": {"const": 2},
                 "values": {"owner_id": {"const": 5}}}]),
        ], partition={"table": "account", "ranges": [[1, 2]]}),
        # Z: genuinely independent partition on `note`; owner 9 still exists,
        # so this batch stays skippable.
        prog("Z", [
            _read("n", "note", [7], {"kind": "all"}),
            _write("replace", "note", "n", [
                {"source_row_id": 7, "row_id": {"const": 7},
                 "values": {"owner_id": {"const": 9}}}]),
        ], partition={"table": "note", "ranges": [[7, 7]]}),
    ]
    return initial, batches
