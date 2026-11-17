"""SQLite physical store: managed rows by generation, atomic head pointer, ledger.

Physical model
--------------
* ``managed_rows(generation, tbl, row_id, row_json)`` stores the complete data
  of each committed generation.  A committed generation is immutable.
* ``generation_head`` has exactly one mutable row.  Committing = inserting new
  generation rows plus switching this pointer in the *same* SQLite
  transaction.  Visitors always resolve one generation number from the head and
  then read only that generation, so a reader sees a whole old or a whole new
  state, never a half-applied revocation.
* ``ledger_batch`` is the append-only anchor of original batch evidence.  It is
  inserted in the same transaction as the batch's row effects.
* ``ingest_record`` is committed before execution.  Once a logical id is
  ingested, its program bytes and its immutable position in time can never
  change.
* Revocation plans, mappings and diffs live beside the official history.  Plan
  construction never touches ``managed_rows``/``generation``/``generation_head``.

Integrity triggers make original evidence physically read-only.
"""

import datetime
import json
import os
import sqlite3

from .canon import canon, content_id, sha256
from .errors import IntegrityError, LedgerError
from .interpreter import state_hash, state_snapshot

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS managed_rows (
    generation INTEGER NOT NULL,
    tbl      TEXT    NOT NULL,
    row_id   INTEGER NOT NULL,
    row_json TEXT    NOT NULL,
    PRIMARY KEY (generation, tbl, row_id)
);

CREATE TABLE IF NOT EXISTS generation_head (
    singleton    INTEGER PRIMARY KEY CHECK (singleton = 1),
    epoch        INTEGER NOT NULL,
    generation   INTEGER NOT NULL,
    state_hash   TEXT    NOT NULL,
    state_digest TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS generation (
    generation    INTEGER PRIMARY KEY,
    epoch         INTEGER NOT NULL,
    parent        INTEGER REFERENCES generation(generation),
    kind          TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    state_hash    TEXT NOT NULL,
    state_digest  TEXT NOT NULL,
    row_counts    TEXT NOT NULL,
    batch_ord     INTEGER,
    revocation_id TEXT,
    cause         TEXT NOT NULL,
    trace_json    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_record (
    logical_batch_id TEXT PRIMARY KEY,
    ingest_seq       INTEGER NOT NULL UNIQUE,
    batch_ord        INTEGER NOT NULL UNIQUE,
    program_json     TEXT NOT NULL,
    program_hash     TEXT NOT NULL,
    ingested_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger_batch (
    batch_ord            INTEGER PRIMARY KEY,
    logical_batch_id     TEXT NOT NULL UNIQUE,
    generation           INTEGER,
    parent_generation    INTEGER NOT NULL,
    status               TEXT NOT NULL CHECK (status IN ('success','failed')),
    program_json         TEXT NOT NULL,
    program_hash         TEXT NOT NULL,
    read_footprint_json  TEXT NOT NULL,
    write_footprint_json TEXT NOT NULL,
    evidence_json        TEXT NOT NULL,
    outcome_json         TEXT NOT NULL,
    abort_json           TEXT,
    key_ranges_json      TEXT,
    recorded_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS revocation (
    revocation_id       TEXT PRIMARY KEY,
    target_batch_ord    INTEGER NOT NULL,
    target_logical_id   TEXT NOT NULL,
    target_generation   INTEGER NOT NULL,
    request_digest      TEXT NOT NULL,
    request_json        TEXT NOT NULL,
    base_generation     INTEGER NOT NULL,
    base_epoch          INTEGER NOT NULL,
    state_promise       TEXT NOT NULL,
    created_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS generation_plan (
    plan_id             TEXT PRIMARY KEY,
    revocation_id       TEXT NOT NULL UNIQUE,
    phase               TEXT NOT NULL CHECK (phase IN
                            ('planning','planned','applied')),
    current_generation  INTEGER NOT NULL,
    current_epoch       INTEGER NOT NULL,
    current_promise     TEXT NOT NULL,
    base_promise        TEXT NOT NULL,
    upstream_end        INTEGER NOT NULL,
    first_target_ord    INTEGER NOT NULL,
    plan_payload_json   TEXT NOT NULL,
    plan_digest         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    planned_generation  INTEGER,
    applied_at          TEXT,
    applied_epoch       INTEGER,
    result_json         TEXT,
    FOREIGN KEY (revocation_id) REFERENCES revocation(revocation_id)
);

CREATE TABLE IF NOT EXISTS revocation_mapping (
    plan_id          TEXT NOT NULL,
    batch_ord        INTEGER NOT NULL,
    logical_batch_id TEXT NOT NULL,
    disposition      TEXT NOT NULL CHECK (disposition IN
                        ('Superseded','Kept','Changed','Blocked')),
    reason_code      TEXT NOT NULL,
    reuse_kind       TEXT,
    detail_json      TEXT NOT NULL,
    PRIMARY KEY (plan_id, batch_ord)
);

CREATE TABLE IF NOT EXISTS revocation_diff (
    plan_id    TEXT NOT NULL,
    scope      TEXT NOT NULL,
    tbl        TEXT NOT NULL,
    row_id     INTEGER NOT NULL,
    old_digest TEXT,
    new_digest TEXT,
    cause      TEXT NOT NULL,
    PRIMARY KEY (plan_id, scope, tbl, row_id)
);

CREATE TABLE IF NOT EXISTS lineage (
    rev_id         TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    generation     INTEGER NOT NULL,
    produced_by_id TEXT NOT NULL,
    disposition    TEXT NOT NULL,
    reason_code    TEXT NOT NULL,
    PRIMARY KEY (rev_id, seq)
);

CREATE TABLE IF NOT EXISTS constraint_config (
    singleton    INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema       TEXT NOT NULL,
    rules_json   TEXT NOT NULL,
    config_hash  TEXT NOT NULL,
    installed_at TEXT NOT NULL,
    installed_at_generation INTEGER NOT NULL,
    installed_before_ingest INTEGER NOT NULL
);

CREATE TRIGGER IF NOT EXISTS ledger_batch_no_update
BEFORE UPDATE ON ledger_batch
BEGIN SELECT RAISE(ABORT, 'ledger_batch is append-only'); END;
CREATE TRIGGER IF NOT EXISTS ledger_batch_no_delete
BEFORE DELETE ON ledger_batch
BEGIN SELECT RAISE(ABORT, 'ledger_batch is append-only'); END;
CREATE TRIGGER IF NOT EXISTS ingest_no_update
BEFORE UPDATE ON ingest_record
BEGIN SELECT RAISE(ABORT, 'ingest_record is append-only'); END;
CREATE TRIGGER IF NOT EXISTS ingest_no_delete
BEFORE DELETE ON ingest_record
BEGIN SELECT RAISE(ABORT, 'ingest_record is append-only'); END;
CREATE TRIGGER IF NOT EXISTS generation_no_delete
BEFORE DELETE ON generation
BEGIN SELECT RAISE(ABORT, 'generation history is append-only'); END;
CREATE TRIGGER IF NOT EXISTS generation_no_update
BEFORE UPDATE ON generation
BEGIN SELECT RAISE(ABORT, 'generation history is read-only'); END;
CREATE TRIGGER IF NOT EXISTS managed_rows_no_update
BEFORE UPDATE ON managed_rows
BEGIN SELECT RAISE(ABORT, 'managed_rows are immutable'); END;
CREATE TRIGGER IF NOT EXISTS managed_rows_no_delete
BEFORE DELETE ON managed_rows
BEGIN SELECT RAISE(ABORT, 'managed_rows are append-only'); END;
CREATE TRIGGER IF NOT EXISTS revocation_no_update
BEFORE UPDATE ON revocation
BEGIN SELECT RAISE(ABORT, 'revocation records are immutable'); END;
CREATE TRIGGER IF NOT EXISTS revocation_no_delete
BEFORE DELETE ON revocation
BEGIN SELECT RAISE(ABORT, 'revocation records are immutable'); END;
CREATE TRIGGER IF NOT EXISTS mapping_no_delete
BEFORE DELETE ON revocation_mapping
WHEN (SELECT phase FROM generation_plan WHERE plan_id=OLD.plan_id)='applied'
BEGIN SELECT RAISE(ABORT, 'revocation_mapping of an applied plan is immutable'); END;
CREATE TRIGGER IF NOT EXISTS diff_no_update
BEFORE UPDATE ON revocation_diff
BEGIN SELECT RAISE(ABORT, 'revocation_diff is append-only'); END;
CREATE TRIGGER IF NOT EXISTS diff_no_delete
BEFORE DELETE ON revocation_diff
WHEN (SELECT phase FROM generation_plan WHERE plan_id=OLD.plan_id)='applied'
BEGIN SELECT RAISE(ABORT, 'revocation_diff of an applied plan is immutable'); END;
CREATE TRIGGER IF NOT EXISTS lineage_no_delete
BEFORE DELETE ON lineage
BEGIN SELECT RAISE(ABORT, 'lineage is append-only'); END;

CREATE TRIGGER IF NOT EXISTS constraint_config_no_update
BEFORE UPDATE ON constraint_config
BEGIN SELECT RAISE(ABORT, 'constraint rules are installed once and immutable'); END;
CREATE TRIGGER IF NOT EXISTS constraint_config_no_delete
BEFORE DELETE ON constraint_config
BEGIN SELECT RAISE(ABORT, 'constraint rules are installed once and immutable'); END;
"""

# Offline CDC transaction ingestor tables.  Created lazily on first use, so a
# database produced by an older build opens and runs unchanged (schema upgrade
# only ever adds tables; no historical table is altered).  The CDC tables are
# deliberately outside ManagedDB.fingerprint(): they are the ingestor's own
# receive-history apparatus, while every official managed effect still flows
# through the existing generation / ledger_batch machinery (which is covered).
CDC_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cdc_source (
    source_id        TEXT PRIMARY KEY,
    start_seq        INTEGER NOT NULL,
    config_json      TEXT NOT NULL,
    config_hash      TEXT NOT NULL,
    installed_at     TEXT NOT NULL,
    installed_at_generation INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS cdc_source_table (
    source_id     TEXT NOT NULL,
    source_table  TEXT NOT NULL,
    managed_table TEXT NOT NULL,
    key_fields    TEXT NOT NULL,
    ord           INTEGER NOT NULL,
    PRIMARY KEY (source_id, source_table),
    UNIQUE (source_id, managed_table)
);

CREATE TABLE IF NOT EXISTS cdc_source_ref (
    source_id    TEXT NOT NULL,
    source_table TEXT NOT NULL,
    column_name  TEXT NOT NULL,
    ref_table    TEXT NOT NULL,
    ord          INTEGER NOT NULL,
    PRIMARY KEY (source_id, source_table, column_name)
);

CREATE TABLE IF NOT EXISTS cdc_fragment (
    source_id     TEXT NOT NULL,
    tx_seq        INTEGER NOT NULL,
    tx_id         TEXT NOT NULL,
    part          TEXT NOT NULL CHECK (part IN ('begin','change','end')),
    change_index  INTEGER,
    semantic_hash TEXT NOT NULL,
    fragment_json TEXT NOT NULL,
    received_at   TEXT NOT NULL,
    PRIMARY KEY (source_id, tx_seq, part, change_index)
);

-- The one-to-one binding of tx_seq and tx_id is kept in its own table: one
-- transaction owns many fragment rows, so the uniqueness cannot live on the
-- fragment table itself.
CREATE TABLE IF NOT EXISTS cdc_tx_binding (
    source_id TEXT NOT NULL,
    tx_seq    INTEGER NOT NULL,
    tx_id     TEXT NOT NULL,
    PRIMARY KEY (source_id, tx_seq),
    UNIQUE (source_id, tx_id)
);

CREATE TABLE IF NOT EXISTS cdc_delivery (
    delivery_id   TEXT PRIMARY KEY,
    source_id     TEXT NOT NULL,
    fragment_count INTEGER NOT NULL,
    digest        TEXT NOT NULL,
    result_json   TEXT NOT NULL,
    delivered_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cdc_cursor (
    source_id    TEXT PRIMARY KEY,
    next_seq     INTEGER NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cdc_identity (
    source_id     TEXT NOT NULL,
    source_table  TEXT NOT NULL,
    key_json      TEXT NOT NULL,
    managed_table TEXT NOT NULL,
    row_id        INTEGER NOT NULL,
    created_tx_seq INTEGER NOT NULL,
    PRIMARY KEY (source_id, source_table, key_json)
);

CREATE TABLE IF NOT EXISTS cdc_allocator (
    source_id     TEXT NOT NULL,
    managed_table TEXT NOT NULL,
    high_water    INTEGER NOT NULL,
    PRIMARY KEY (source_id, managed_table)
);

CREATE TABLE IF NOT EXISTS cdc_tx_decision (
    source_id      TEXT NOT NULL,
    tx_seq         INTEGER NOT NULL,
    tx_id          TEXT NOT NULL,
    outcome        TEXT NOT NULL CHECK (outcome IN
                     ('committed','aborted','rejected')),
    decision_digest TEXT NOT NULL,
    summary_json   TEXT NOT NULL,
    detail_json    TEXT NOT NULL,
    decided_at     TEXT NOT NULL,
    PRIMARY KEY (source_id, tx_seq)
);

CREATE TABLE IF NOT EXISTS cdc_skip (
    source_id     TEXT NOT NULL,
    tx_seq        INTEGER NOT NULL,
    reason        TEXT NOT NULL,
    decision_digest TEXT NOT NULL,
    skipped_at    TEXT NOT NULL,
    PRIMARY KEY (source_id, tx_seq)
);

CREATE TABLE IF NOT EXISTS cdc_batch_link (
    source_id      TEXT NOT NULL,
    tx_seq         INTEGER NOT NULL,
    tx_id          TEXT NOT NULL,
    batch_ord      INTEGER NOT NULL,
    logical_batch_id TEXT NOT NULL,
    PRIMARY KEY (source_id, tx_seq),
    UNIQUE (batch_ord)
);

CREATE TABLE IF NOT EXISTS cdc_change_map (
    source_id     TEXT NOT NULL,
    tx_seq        INTEGER NOT NULL,
    change_index  INTEGER NOT NULL,
    source_table  TEXT NOT NULL,
    managed_table TEXT NOT NULL,
    op            TEXT NOT NULL,
    key_json      TEXT NOT NULL,
    row_id        INTEGER NOT NULL,
    PRIMARY KEY (source_id, tx_seq, change_index)
);

CREATE TRIGGER IF NOT EXISTS cdc_fragment_no_update
BEFORE UPDATE ON cdc_fragment
BEGIN SELECT RAISE(ABORT, 'cdc_fragment is append-only'); END;
CREATE TRIGGER IF NOT EXISTS cdc_fragment_no_delete
BEFORE DELETE ON cdc_fragment
BEGIN SELECT RAISE(ABORT, 'cdc_fragment is append-only'); END;
CREATE TRIGGER IF NOT EXISTS cdc_binding_no_update
BEFORE UPDATE ON cdc_tx_binding
BEGIN SELECT RAISE(ABORT, 'cdc tx_seq/tx_id bindings are immutable'); END;
CREATE TRIGGER IF NOT EXISTS cdc_binding_no_delete
BEFORE DELETE ON cdc_tx_binding
BEGIN SELECT RAISE(ABORT, 'cdc tx_seq/tx_id bindings are immutable'); END;
CREATE TRIGGER IF NOT EXISTS cdc_source_no_update
BEFORE UPDATE ON cdc_source
BEGIN SELECT RAISE(ABORT, 'cdc_source configuration is immutable'); END;
CREATE TRIGGER IF NOT EXISTS cdc_source_no_delete
BEFORE DELETE ON cdc_source
BEGIN SELECT RAISE(ABORT, 'cdc_source configuration is immutable'); END;
CREATE TRIGGER IF NOT EXISTS cdc_decision_no_update
BEFORE UPDATE ON cdc_tx_decision
BEGIN SELECT RAISE(ABORT, 'cdc_tx_decision is append-only'); END;
CREATE TRIGGER IF NOT EXISTS cdc_decision_no_delete
BEFORE DELETE ON cdc_tx_decision
BEGIN SELECT RAISE(ABORT, 'cdc_tx_decision is append-only'); END;
CREATE TRIGGER IF NOT EXISTS cdc_identity_no_update
BEFORE UPDATE ON cdc_identity
BEGIN SELECT RAISE(ABORT, 'cdc_identity mappings are immutable'); END;
CREATE TRIGGER IF NOT EXISTS cdc_identity_no_delete
BEFORE DELETE ON cdc_identity
BEGIN SELECT RAISE(ABORT, 'cdc_identity mappings are retained even after deletes'); END;
CREATE TRIGGER IF NOT EXISTS cdc_link_no_update
BEFORE UPDATE ON cdc_batch_link
BEGIN SELECT RAISE(ABORT, 'cdc_batch_link is append-only'); END;
CREATE TRIGGER IF NOT EXISTS cdc_link_no_delete
BEFORE DELETE ON cdc_batch_link
BEGIN SELECT RAISE(ABORT, 'cdc_batch_link is append-only'); END;
CREATE TRIGGER IF NOT EXISTS cdc_change_map_no_update
BEFORE UPDATE ON cdc_change_map
BEGIN SELECT RAISE(ABORT, 'cdc_change_map is append-only'); END;
CREATE TRIGGER IF NOT EXISTS cdc_change_map_no_delete
BEFORE DELETE ON cdc_change_map
BEGIN SELECT RAISE(ABORT, 'cdc_change_map is append-only'); END;
"""


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ")


class Tx:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")
        return False


class PhysicalState:
    """Mutable working copy used by the interpreter for one execution."""

    def __init__(self, rows=None, tables=()):
        self._rows = {(t, int(r)): dict(v) for (t, r), v in (rows or {}).items()}
        self._tables = set(tables)

    @classmethod
    def from_generation(cls, db, generation):
        info = db.generation_info(generation)
        st = cls(tables=info["row_counts"].keys())
        for tbl, rid, row in db.rows_for_generation(generation):
            st._rows[(tbl, rid)] = row
            st._tables.add(tbl)
        return st

    def managed_tables(self):
        return sorted(self._tables)

    def tables(self):
        return sorted(self._tables)

    def ensure_table(self, table):
        self._tables.add(table)

    def row_ids(self, table):
        return sorted(r for (t, r) in self._rows if t == table)

    def get_row(self, table, row_id):
        return self._rows.get((table, row_id))

    def put_row(self, table, row_id, row):
        self._tables.add(table)
        self._rows[(table, row_id)] = dict(row)

    def delete_row(self, table, row_id):
        self._rows.pop((table, row_id), None)

    def copy(self):
        return PhysicalState({k: dict(v) for k, v in self._rows.items()}, self._tables)

    def snapshot_rows(self):
        return {(t, r): dict(v) for (t, r), v in self._rows.items()}

    def state_hash(self):
        return state_hash(self)

    def state_digest(self):
        return content_id("state", state_snapshot(self))


class ManagedDB:
    """Thin physical layer.  Business rules live in Ledger (ledger.py)."""

    def __init__(self, path, create=True):
        self.path = path
        exists = os.path.exists(path) and os.path.getsize(path) > 0
        if not exists and not create:
            raise LedgerError(f"database {path!r} does not exist")
        self.conn = sqlite3.connect(path, isolation_level=None, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=FULL")
        if not exists:
            self.conn.executescript(SCHEMA_SQL)
            with self.tx():
                self.conn.execute(
                    "INSERT INTO meta(key,value) VALUES('ingest_seq','0')")
                self._create_generation_locked(
                    parent=None, kind="genesis", cause="genesis",
                    state=PhysicalState(), batch_ord=None, revocation_id=None,
                    trace={"events": [{"event": "genesis"}]})
        else:
            # Forward-only upgrade for databases created by older builds: the
            # CDC apparatus is added on first use; no historical table, row or
            # trigger is modified or dropped.
            self.ensure_cdc_schema()

    def ensure_cdc_schema(self):
        """Idempotently create the CDC tables on an existing database."""
        self.conn.executescript(CDC_SCHEMA_SQL)

    # ------------------------------------------------------------------ tx
    def tx(self):
        return Tx(self.conn)

    def close(self):
        self.conn.close()

    # ------------------------------------------------------------- reads
    def head(self):
        r = self.conn.execute(
            "SELECT * FROM generation_head WHERE singleton=1").fetchone()
        return {"epoch": r["epoch"], "generation": r["generation"],
                "state_hash": r["state_hash"], "state_digest": r["state_digest"]}

    def generation_info(self, generation):
        r = self.conn.execute("SELECT * FROM generation WHERE generation=?",
                              (generation,)).fetchone()
        if r is None:
            raise IntegrityError(f"unknown generation {generation}")
        return {
            "generation": r["generation"], "epoch": r["epoch"],
            "parent": r["parent"], "kind": r["kind"],
            "state_hash": r["state_hash"], "state_digest": r["state_digest"],
            "row_counts": json.loads(r["row_counts"]),
            "batch_ord": r["batch_ord"], "revocation_id": r["revocation_id"],
            "cause": r["cause"], "created_at": r["created_at"],
            "trace": json.loads(r["trace_json"]),
        }

    def generations(self):
        return [self.generation_info(r["generation"]) for r in self.conn.execute(
            "SELECT generation FROM generation ORDER BY generation")]

    def rows_for_generation(self, generation):
        cur = self.conn.execute(
            "SELECT tbl,row_id,row_json FROM managed_rows WHERE generation=? "
            "ORDER BY tbl,row_id", (generation,))
        return [(r["tbl"], r["row_id"], json.loads(r["row_json"])) for r in cur]

    def tables_for_generation(self, generation):
        cur = self.conn.execute(
            "SELECT DISTINCT tbl FROM managed_rows WHERE generation=?", (generation,))
        return [r["tbl"] for r in cur]

    def state(self, generation=None):
        gen = self.head()["generation"] if generation is None else generation
        return PhysicalState.from_generation(self, gen)

    def get_row_at(self, generation, table, row_id):
        r = self.conn.execute(
            "SELECT row_json FROM managed_rows WHERE generation=? AND tbl=? AND row_id=?",
            (generation, table, int(row_id))).fetchone()
        return None if r is None else json.loads(r["row_json"])

    # ------------------------------------------------------------ writes
    def _insert_generation_rows(self, generation, state):
        for (tbl, rid), row in sorted(state.snapshot_rows().items()):
            self.conn.execute(
                "INSERT INTO managed_rows(generation,tbl,row_id,row_json) "
                "VALUES(?,?,?,?)", (generation, tbl, int(rid), canon(row)))

    def _create_generation_locked(self, parent, kind, cause, state, batch_ord,
                                  revocation_id, trace, epoch_bump=True):
        head = self.conn.execute(
            "SELECT * FROM generation_head WHERE singleton=1").fetchone()
        cur_epoch = head["epoch"] if head else 0
        cur_gen = head["generation"] if head else 0
        gen = cur_gen + 1
        epoch = cur_epoch + 1 if epoch_bump else cur_epoch
        self._insert_generation_rows(gen, state)
        sh = state.state_hash()
        sd = state.state_digest()
        counts = {t: len(state.row_ids(t)) for t in state.tables()}
        self.conn.execute(
            "INSERT INTO generation(generation,epoch,parent,kind,created_at,state_hash,"
            "state_digest,row_counts,batch_ord,revocation_id,cause,trace_json) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (gen, epoch, parent, kind, utc_now(), sh, sd, canon(counts),
             batch_ord, revocation_id, cause, canon(trace)))
        if head is None:
            self.conn.execute(
                "INSERT INTO generation_head(singleton,epoch,generation,state_hash,"
                "state_digest) VALUES(1,?,?,?,?)", (epoch, gen, sh, sd))
        else:
            self.conn.execute(
                "UPDATE generation_head SET epoch=?, generation=?, state_hash=?, "
                "state_digest=? WHERE singleton=1", (epoch, gen, sh, sd))
        return {"generation": gen, "epoch": epoch, "state_hash": sh,
                "state_digest": sd, "row_counts": counts}

    def commit_state(self, state, kind, cause, parent=None, batch_ord=None,
                     revocation_id=None, trace=None):
        """Commit one new generation and switch the head atomically."""
        with self.tx():
            info = self._create_generation_locked(
                parent=parent, kind=kind, cause=cause, state=state,
                batch_ord=batch_ord, revocation_id=revocation_id,
                trace=trace or {"events": []})
        return info

    # ------------------------------------------------------------ ledger
    def batches(self, statuses=None):
        q = "SELECT * FROM ledger_batch"
        args = []
        if statuses:
            q += " WHERE status IN (%s)" % ",".join("?" for _ in statuses)
            args = list(statuses)
        q += " ORDER BY batch_ord"
        return [self._batch_row(r) for r in self.conn.execute(q, args)]

    def batch_by_ord(self, batch_ord):
        r = self.conn.execute("SELECT * FROM ledger_batch WHERE batch_ord=?",
                              (batch_ord,)).fetchone()
        return None if r is None else self._batch_row(r)

    def batch_by_logical_id(self, logical_id):
        r = self.conn.execute("SELECT * FROM ledger_batch WHERE logical_batch_id=?",
                              (logical_id,)).fetchone()
        return None if r is None else self._batch_row(r)

    @staticmethod
    def _batch_row(r):
        return {
            "batch_ord": r["batch_ord"],
            "logical_batch_id": r["logical_batch_id"],
            "generation": r["generation"],
            "parent_generation": r["parent_generation"],
            "status": r["status"],
            "program": json.loads(r["program_json"]),
            "program_hash": r["program_hash"],
            "read_footprint": json.loads(r["read_footprint_json"]),
            "write_footprint": json.loads(r["write_footprint_json"]),
            "evidence": json.loads(r["evidence_json"]),
            "outcome": json.loads(r["outcome_json"]),
            "abort": json.loads(r["abort_json"]) if r["abort_json"] else None,
            "key_ranges": json.loads(r["key_ranges_json"]) if r["key_ranges_json"] else None,
            "recorded_at": r["recorded_at"],
        }

    def ingest_row(self, logical_id):
        r = self.conn.execute("SELECT * FROM ingest_record WHERE logical_batch_id=?",
                              (logical_id,)).fetchone()
        if r is None:
            return None
        return {"logical_batch_id": r["logical_batch_id"],
                "ingest_seq": r["ingest_seq"], "batch_ord": r["batch_ord"],
                "program": json.loads(r["program_json"]),
                "program_hash": r["program_hash"],
                "ingested_at": r["ingested_at"]}

    def ingest_batches(self):
        return [{"ingest_seq": r["ingest_seq"], "batch_ord": r["batch_ord"],
                 "logical_batch_id": r["logical_batch_id"],
                 "program_hash": r["program_hash"]}
                for r in self.conn.execute(
                    "SELECT * FROM ingest_record ORDER BY ingest_seq")]

    def append_ingest(self, logical_id, program, program_hash):
        with self.tx():
            batch_ord = self._insert_ingest_locked(logical_id, program,
                                                   program_hash)
        return batch_ord

    def _insert_ingest_locked(self, logical_id, program, program_hash):
        """Insert an ingest record inside a caller-held transaction.

        The CDC advance must never leave a half-pending program: its ingest
        row, ledger decision, generation and source cursor commit together, so
        the CDC ingestor calls this inside its own single transaction.
        """
        r = self.conn.execute(
            "SELECT MAX(batch_ord) AS m FROM ingest_record").fetchone()
        batch_ord = (r["m"] or 0) + 1
        seq = int(self.conn.execute(
            "SELECT value FROM meta WHERE key='ingest_seq'").fetchone()["value"]) + 1
        self.conn.execute("UPDATE meta SET value=? WHERE key='ingest_seq'",
                          (str(seq),))
        self.conn.execute(
            "INSERT INTO ingest_record(logical_batch_id,ingest_seq,batch_ord,"
            "program_json,program_hash,ingested_at) VALUES(?,?,?,?,?,?)",
            (logical_id, seq, batch_ord, canon(program), program_hash,
             utc_now()))
        return batch_ord

    def append_ledger(self, raw, evidence, outcome, abort, program_hash,
                      parent_generation, status, generation, key_ranges=None):
        self.conn.execute(
            "INSERT INTO ledger_batch(batch_ord,logical_batch_id,generation,"
            "parent_generation,status,program_json,program_hash,read_footprint_json,"
            "write_footprint_json,evidence_json,outcome_json,abort_json,"
            "key_ranges_json,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (raw["batch_ord"], raw["logical_batch_id"], generation,
             parent_generation, status, canon(raw["program"]), program_hash,
             canon(evidence["read_footprint"]), canon(evidence["write_footprint"]),
             canon(evidence), canon(outcome),
             canon(abort) if abort is not None else None,
             canon(key_ranges) if key_ranges is not None else None, utc_now()))

    # -------------------------------------------------- constraint rules
    def constraint_config(self):
        r = self.conn.execute(
            "SELECT * FROM constraint_config WHERE singleton=1").fetchone()
        if r is None:
            return None
        return {
            "schema": r["schema"],
            "rules": json.loads(r["rules_json"]),
            "config_hash": r["config_hash"],
            "installed_at": r["installed_at"],
            "installed_at_generation": r["installed_at_generation"],
            "installed_before_ingest": bool(r["installed_before_ingest"]),
        }

    def install_constraint_config(self, config_json, config_hash, generation):
        """Persist the rule set once.  Refuses any second installation."""
        with self.tx():
            if self.constraint_config() is not None:
                raise LedgerError("constraint rules are already installed and "
                                  "cannot be replaced")
            ingested = self.conn.execute(
                "SELECT COUNT(*) AS n FROM ingest_record").fetchone()["n"]
            if ingested:
                raise LedgerError("constraint rules must be installed before the "
                                  "first ingest")
            info = self.generation_info(generation)
            if info["kind"] != "genesis" or info["row_counts"]:
                raise LedgerError("constraint rules must be installed before the "
                                  "seed")
            self.conn.execute(
                "INSERT INTO constraint_config(singleton,schema,rules_json,"
                "config_hash,installed_at,installed_at_generation,"
                "installed_before_ingest) VALUES(1,?,?,?,?,?,1)",
                (json.loads(config_json)["schema"],
                 canon(json.loads(config_json)["rules"]), config_hash,
                 utc_now(), generation))
        return self.constraint_config()

    def config_snapshot(self):
        """Promise over the installed rule set (empty when unconfigured)."""
        raw = self.conn.execute(
            "SELECT config_hash FROM constraint_config WHERE singleton=1").fetchone()
        if raw is None:
            return {"installed": False, "config_hash": None, "schema": None,
                    "rule_count": 0, "rules": []}
        cfg = self.constraint_config()
        return {"installed": True, "config_hash": cfg["config_hash"],
                "schema": cfg["schema"], "rule_count": len(cfg["rules"]),
                "rules": cfg["rules"]}


    # -------------------------------------------------------- revocations
    def revocation(self, revocation_id):
        r = self.conn.execute("SELECT * FROM revocation WHERE revocation_id=?",
                              (revocation_id,)).fetchone()
        if r is None:
            return None
        return {
            "revocation_id": r["revocation_id"],
            "target_batch_ord": r["target_batch_ord"],
            "target_logical_id": r["target_logical_id"],
            "target_generation": r["target_generation"],
            "request_digest": r["request_digest"],
            "request": json.loads(r["request_json"]),
            "base_generation": r["base_generation"],
            "base_epoch": r["base_epoch"],
            "state_promise": r["state_promise"],
            "created_at": r["created_at"],
        }

    def revocations(self):
        return [self.revocation(r["revocation_id"]) for r in self.conn.execute(
            "SELECT revocation_id FROM revocation ORDER BY created_at")]

    def plan(self, plan_id):
        r = self.conn.execute("SELECT * FROM generation_plan WHERE plan_id=?",
                              (plan_id,)).fetchone()
        return None if r is None else self._plan_row(r)

    def plan_for_revocation(self, revocation_id):
        r = self.conn.execute(
            "SELECT * FROM generation_plan WHERE revocation_id=?",
            (revocation_id,)).fetchone()
        return None if r is None else self._plan_row(r)

    def plans(self):
        return [self._plan_row(r) for r in self.conn.execute(
            "SELECT * FROM generation_plan ORDER BY created_at, plan_id")]

    @staticmethod
    def _plan_row(r):
        return {
            "plan_id": r["plan_id"],
            "revocation_id": r["revocation_id"],
            "phase": r["phase"],
            "current_generation": r["current_generation"],
            "current_epoch": r["current_epoch"],
            "current_promise": r["current_promise"],
            "base_promise": r["base_promise"],
            "upstream_end": r["upstream_end"],
            "first_target_ord": r["first_target_ord"],
            "payload": json.loads(r["plan_payload_json"]),
            "plan_digest": r["plan_digest"],
            "created_at": r["created_at"],
            "planned_generation": r["planned_generation"],
            "applied_at": r["applied_at"],
            "applied_epoch": r["applied_epoch"],
            "result": json.loads(r["result_json"]) if r["result_json"] else None,
        }

    def mapping(self, plan_id):
        return [{"batch_ord": r["batch_ord"],
                 "logical_batch_id": r["logical_batch_id"],
                 "disposition": r["disposition"],
                 "reason_code": r["reason_code"],
                 "reuse_kind": r["reuse_kind"],
                 "detail": json.loads(r["detail_json"])}
                for r in self.conn.execute(
                    "SELECT * FROM revocation_mapping WHERE plan_id=? "
                    "ORDER BY batch_ord", (plan_id,))]

    def diff(self, plan_id, scope="head"):
        return [{"table": r["tbl"], "row_id": r["row_id"],
                 "old": r["old_digest"], "new": r["new_digest"],
                 "cause": r["cause"]}
                for r in self.conn.execute(
                    "SELECT * FROM revocation_diff WHERE plan_id=? AND scope=? "
                    "ORDER BY tbl,row_id", (plan_id, scope))]

    # --------------------------------------------------------- integrity
    def fingerprint(self):
        """Promise over the official managed world, used to detect staleness.

        Covers: every generation's row bytes, the head pointer/epoch, ingest
        records, the batch ledger (original evidence), generation metadata and
        meta counters.  A new ingest, a new batch decision, a new generation, a
        head switch, or any out-of-band SQLite edit of those tables changes it.

        Deliberately excludes the revocation/plan bookkeeping tables: those are
        the plan's own apparatus, are validated separately (immutable revocation
        binding, plan payload digest, applied-once guard), and must not make a
        freshly written plan stale against itself.
        """
        parts = {}
        parts["managed_rows"] = [
            [r["generation"], r["tbl"], r["row_id"], sha256(r["row_json"])]
            for r in self.conn.execute(
                "SELECT generation,tbl,row_id,row_json FROM managed_rows "
                "ORDER BY generation,tbl,row_id")]
        parts["ingest_record"] = [
            [r["batch_ord"], r["logical_batch_id"], r["program_hash"],
             sha256(r["program_json"])]
            for r in self.conn.execute(
                "SELECT * FROM ingest_record ORDER BY batch_ord")]
        parts["ledger_batch"] = [
            [r["batch_ord"], r["logical_batch_id"], r["status"], r["program_hash"],
             r["generation"], r["parent_generation"],
             sha256(r["evidence_json"]), sha256(r["outcome_json"]),
             sha256(r["abort_json"]) if r["abort_json"] else None]
            for r in self.conn.execute("SELECT * FROM ledger_batch ORDER BY batch_ord")]
        parts["generation_head"] = self.head()
        parts["generation"] = [
            [r["generation"], r["epoch"], r["parent"], r["kind"], r["state_hash"],
             r["state_digest"], sha256(r["row_counts"]), r["batch_ord"],
             r["revocation_id"], sha256(r["trace_json"])]
            for r in self.conn.execute("SELECT * FROM generation ORDER BY generation")]
        parts["meta"] = [[r["key"], r["value"]] for r in self.conn.execute(
            "SELECT * FROM meta ORDER BY key")]
        parts["constraint_config"] = self.config_snapshot()
        return sha256(parts)

    def audit_fingerprint(self):
        """Complete database promise including plans/mappings/diffs (audit only)."""
        parts = {"official": self.fingerprint()}
        parts["revocation"] = [
            [r["revocation_id"], r["target_batch_ord"], r["target_logical_id"],
             r["request_digest"], r["base_generation"], r["state_promise"],
             sha256(r["request_json"])]
            for r in self.conn.execute("SELECT * FROM revocation ORDER BY revocation_id")]
        parts["generation_plan"] = [
            [r["plan_id"], r["revocation_id"], r["phase"], r["plan_digest"],
             r["current_generation"], r["planned_generation"],
             sha256(r["plan_payload_json"]),
             sha256(r["result_json"]) if r["result_json"] else None]
            for r in self.conn.execute(
                "SELECT * FROM generation_plan ORDER BY plan_id")]
        parts["revocation_mapping"] = [
            [r["plan_id"], r["batch_ord"], r["disposition"], r["reason_code"],
             r["reuse_kind"], sha256(r["detail_json"])]
            for r in self.conn.execute(
                "SELECT * FROM revocation_mapping ORDER BY plan_id,batch_ord")]
        parts["revocation_diff"] = [
            [r["plan_id"], r["scope"], r["tbl"], r["row_id"], r["old_digest"],
             r["new_digest"], r["cause"]]
            for r in self.conn.execute(
                "SELECT * FROM revocation_diff ORDER BY plan_id,scope,tbl,row_id")]
        parts["lineage"] = [
            [r["rev_id"], r["seq"], r["generation"], r["produced_by_id"],
             r["disposition"], r["reason_code"]]
            for r in self.conn.execute("SELECT * FROM lineage ORDER BY rev_id,seq")]
        return sha256(parts)


def state_promise(state):
    """Promise for one state, symmetric with generation.state_hash/digest."""
    return sha256({"state_hash": state.state_hash(),
                   "state_digest": state.state_digest()})
