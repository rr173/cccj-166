"""Deterministic executor for ``managed-cdc/v1`` transaction programs.

An advanced source transaction is materialized into a closed, versioned
program (see :mod:`revoke_ledger.cdcp`) whose identity allocations and
converted before/after rows are frozen.  This module executes that program
against a managed state, reusing the ledger's exact evidence contract so CDC
commits are ordinary batches:

* changes apply strictly in index order and each change sees the candidate
  results of earlier changes of the *same* transaction;
* every before-image is compared as a *complete* row with type-sensitive
  equality (missing column vs explicit null differ; ``true`` never equals
  ``1``; text ``"1"`` is a third identity), so a target changed by ordinary
  processing or by a revocation aborts the transaction instead of being
  silently overwritten;
* nothing cascades between changes: the existing batch-final constraint gate
  runs once, after the whole source transaction's explicit changes, exactly as
  for every other program schema.

The executor is a pure function of (program, state, constraints); it performs
no I/O and allocates no identities.
"""

from .canon import canon, sha256
from .errors import BatchAbort
from .interpreter import finalize_constraints, row_digest


def typed_equal(a, b):
    """Type-sensitive structural equality for managed rows / scalars.

    Distinguishes int ``1``, bool ``true`` and text ``"1"``, distinguishes a
    missing dictionary key from an explicit null, and otherwise compares
    recursively while preserving list order and element types.
    """
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        if set(a) != set(b):
            return False
        return all(typed_equal(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(
            typed_equal(x, y) for x, y in zip(a, b))
    if a is None:
        return b is None
    # bool/int/str are exact-type compared by the type() guard above.
    return a == b


def _row_digest(row):
    return row_digest(row)


def execute_cdc_program(prog, state, constraints=None):
    """Execute one validated ``managed-cdc/v1`` program on `state`.

    Mutates `state` as it goes; on failure the caller must discard the copy.
    On success the evidence has the same shape as
    :func:`revoke_ledger.interpreter.execute_program`.
    """
    pre_state = state.copy() if constraints is not None else None
    reads_ev, writes_ev = [], []
    read_footprint, write_footprint, read_contents = {}, {}, {}
    action_taken = False
    aborted = None

    # ---- synthetic start-of-transaction reads, one per managed table -------
    by_table = {}
    for ch in prog["changes"]:
        by_table.setdefault(ch["managed_table"], []).append(ch)
    aliases = {}
    for step_index, table in enumerate(sorted(by_table)):
        alias = f"cdc_r{step_index}"
        aliases[table] = alias
        ids = sorted({ch["target_row_id"] for ch in by_table[table]})
        classification, selected, selected_rows = [], [], {}
        for rid in ids:
            row = state.get_row(table, rid)
            if row is None:
                classification.append({"row_id": rid, "status": "absent"})
                continue
            classification.append({"row_id": rid, "status": "selected",
                                   "row": canon(row)})
            selected.append(rid)
            selected_rows[rid] = row
            read_contents.setdefault(table, {})[rid] = row
            read_footprint.setdefault(table, set()).add(rid)
        reads_ev.append({
            "step_index": step_index, "alias": alias, "table": table,
            "where": {"kind": "all"},
            "map": [{"row_id": rid, "table": table} for rid in ids],
            "map_size": len(ids),
            "candidate_count": len(selected),
            "selected_ids": selected, "selected_count": len(selected),
            "selected_digest": sha256(
                [[r, canon(selected_rows[r])] for r in selected]),
            "classification": classification,
        })

    # ---- changes in frozen index order ------------------------------------
    for offset, ch in enumerate(prog["changes"]):
        step_index = len(reads_ev) + offset
        table, rid = ch["managed_table"], ch["target_row_id"]
        current = state.get_row(table, rid)
        op, want_before, want_after = ch["op"], ch["before"], ch["after"]
        before_digest = _row_digest(current)

        def fail(code, message):
            return BatchAbort(
                code, message, step_index=ch["index"], table=table,
                row_id=rid,
                detail={"source_id": prog["source_id"],
                        "tx_seq": prog["tx_seq"], "tx_id": prog["tx_id"],
                        "change_index": ch["index"],
                        "op": op, "target_row_id": rid,
                        "expected_before": want_before,
                        "actual_before": current})

        # 1. operation-specific preconditions, then the full type-sensitive
        # before-image comparison.  An insert whose target already exists is a
        # primary-key conflict (the same vocabulary as managed-batch/v1),
        # distinct from a content-level before mismatch.
        if op == "insert":
            if current is not None:
                aborted = fail(
                    "pk_conflict",
                    f"source tx {prog['tx_id']!r} change {ch['index']}: insert "
                    f"target {table}[{rid}] already exists")
                break
            if want_before is not None:
                aborted = fail(
                    "cdc_before_mismatch",
                    f"source tx {prog['tx_id']!r} change {ch['index']}: insert "
                    f"requires before=null")
                break
            state.put_row(table, rid, want_after)
            after = want_after
        elif op == "delete":
            if current is None:
                aborted = fail(
                    "delete_missing",
                    f"source tx {prog['tx_id']!r} change {ch['index']}: delete "
                    f"target {table}[{rid}] is absent")
                break
            if want_after is not None:
                aborted = fail(
                    "cdc_before_mismatch",
                    f"source tx {prog['tx_id']!r} change {ch['index']}: delete "
                    f"requires after=null")
                break
            if not typed_equal(current, want_before):
                aborted = fail(
                    "cdc_before_mismatch",
                    f"source tx {prog['tx_id']!r} change {ch['index']}: "
                    f"before image of {table}[{rid}] does not match the "
                    f"committed target row")
                break
            state.delete_row(table, rid)
            after = None
        else:  # update: before/after both present, identity already fixed
            if current is None or want_after is None or want_before is None:
                aborted = fail(
                    "cdc_before_mismatch",
                    f"source tx {prog['tx_id']!r} change {ch['index']}: update "
                    f"requires present before and after rows")
                break
            if not typed_equal(current, want_before):
                aborted = fail(
                    "cdc_before_mismatch",
                    f"source tx {prog['tx_id']!r} change {ch['index']}: "
                    f"before image of {table}[{rid}] does not match the "
                    f"committed target row")
                break
            state.put_row(table, rid, want_after)
            after = want_after
        writes_ev.append({
            "step_index": step_index, "effect": op, "table": table,
            "rows_alias": aliases[table],
            "rows": [{"source_row_id": rid, "target_row_id": rid,
                      "before": before_digest, "after": _row_digest(after)}],
        })
        write_footprint.setdefault(table, set()).add(rid)
        action_taken = True

    report = None
    if aborted is None and constraints is not None:
        report, aborted = finalize_constraints(
            constraints, pre_state, state, writes_ev,
            len(reads_ev) + len(writes_ev))

    return {
        "ok": aborted is None,
        "reads": reads_ev,
        "writes": writes_ev,
        "guards": [],
        "read_footprint": {t: sorted(s) for t, s in sorted(read_footprint.items())},
        "write_footprint": {t: sorted(s) for t, s in
                            sorted(write_footprint.items())},
        "read_contents": {t: {str(k): read_contents[t][k]
                              for k in sorted(read_contents[t])}
                          for t in sorted(read_contents)},
        "action_taken": action_taken,
        "constraints": report,
        "abort": aborted.as_dict() if aborted is not None else None,
    }
