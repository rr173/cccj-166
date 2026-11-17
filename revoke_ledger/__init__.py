"""revoke_ledger: managed batch ledger with selective, evidence-based revocation.

Public surface (see README.md for the full protocol and semantics):

    Ledger(path)
    Ledger.install_constraints(config)         -> declarative FK/unique rules
    Ledger.constraints_view()                  -> persistent configuration read-back
    Ledger.seed(rows)                          -> create the initial state
    Ledger.ingest(program, idem_key=None)      -> validate + durably register
    Ledger.run_ingested(logical_batch_id)      -> execute in immutable order
    Ledger.submit(program)                     -> ingest + run
    Ledger.plan_revocation(rev_id, target_id)  -> build a plan (no head change)
    Ledger.apply_plan(plan_id)                 -> atomic new generation
    Ledger.revocation_result(rev_id)            -> idempotent decision lookup
    Ledger.explain_batch(id) / explain_revocation(id)
    Ledger.recover_interrupted_plan(...)        -> resume from persisted stage
    Ledger.head() / snapshots() / history() / ledger()
    Ledger.cdc()                               -> offline CDC transaction ingestor
"""

from .constraints import (CONSTRAINT_SCHEMA, ConstraintSet, validate_config)
from .ledger import Ledger, REASON_TEXT
from .protocol import SCHEMA, validate_program, program_digest
from .rprotocol import RELATIONAL_SCHEMA, validate_relational_program
from .cdcp import (CDC_CONFIG_SCHEMA, CDC_FRAGMENT_SCHEMA, CDC_PROGRAM_SCHEMA,
                   validate_fragment, validate_source_config,
                   validate_cdc_program)
from .cdc import CdcBusyError, CdcIngestor
from .errors import (BatchAbort, IntegrityError, LedgerError, LimitError,
                     ProtocolError, StaleError)

__all__ = [
    "Ledger", "REASON_TEXT", "SCHEMA", "validate_program", "program_digest",
    "CONSTRAINT_SCHEMA", "ConstraintSet", "validate_config",
    "RELATIONAL_SCHEMA", "validate_relational_program",
    "CDC_CONFIG_SCHEMA", "CDC_FRAGMENT_SCHEMA", "CDC_PROGRAM_SCHEMA",
    "validate_fragment", "validate_source_config", "validate_cdc_program",
    "CdcIngestor", "CdcBusyError",
    "BatchAbort", "IntegrityError", "LedgerError", "LimitError", "ProtocolError",
    "StaleError",
]
