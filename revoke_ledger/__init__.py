"""revoke_ledger: managed batch ledger with selective, evidence-based revocation.

Public surface (see README.md for the full protocol and semantics):

    Ledger(path)
    Ledger.seed(rows)                          -> create the initial state
    Ledger.ingest(program, idem_key=None)      -> validate + durably register
    Ledger.run_ingested(logical_batch_id)      -> execute in immutable order
    Ledger.submit(program)                     -> ingest + run
    Ledger.plan_revocation(rev_id, target_id)  -> build a plan (no head change)
    Ledger.apply_plan(plan_id)                 -> atomic new generation
    Ledger.revocation_result(rev_id)            -> idempotent decision lookup
    Ledger.recover_interrupted_plan(...)        -> resume from persisted stage
    Ledger.head() / snapshots() / history() / ledger()
"""

from .ledger import Ledger, REASON_TEXT
from .protocol import SCHEMA, validate_program, program_digest
from .errors import (BatchAbort, IntegrityError, LedgerError, LimitError,
                     ProtocolError, StaleError)

__all__ = [
    "Ledger", "REASON_TEXT", "SCHEMA", "validate_program", "program_digest",
    "BatchAbort", "IntegrityError", "LedgerError", "LimitError", "ProtocolError",
    "StaleError",
]
