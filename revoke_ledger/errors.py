"""Typed errors used across the revocation ledger."""


class LedgerError(Exception):
    """Base error for all ledger/protocol failures."""


class ProtocolError(LedgerError):
    """A batch program or a plan violates the closed protocol."""


class LimitError(ProtocolError):
    """Program exceeds protocol size limits."""


class BatchAbort(LedgerError):
    """A batch aborted while executing (guard/pk/step/state/drift)."""

    def __init__(self, code, message, step_index=None, row_id=None, table=None,
                 detail=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.step_index = step_index
        self.row_id = row_id
        self.table = table
        self.detail = detail or {}

    def as_dict(self):
        return {
            "code": self.code,
            "message": self.message,
            "step_index": self.step_index,
            "row_id": self.row_id,
            "table": self.table,
            "detail": self.detail,
        }


class StaleError(LedgerError):
    """The plan no longer matches the committed generation/inputs."""

    def __init__(self, code, message, detail=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.detail = detail or {}

    def as_dict(self):
        return {"code": self.code, "message": self.message, "detail": self.detail}


class IntegrityError(LedgerError):
    """Stored evidence/state is internally inconsistent."""
