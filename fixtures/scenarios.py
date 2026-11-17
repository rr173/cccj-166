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
