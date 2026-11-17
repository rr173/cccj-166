"""Offline CDC transaction ingestor.

The ingestor turns an unreliable, out-of-order stream of JSONL fragment
*files* (repeated, reordered or truncated in transit) into durable source
transactions and, only when a complete contiguous prefix is available, into
ordinary managed ledger batches.

Stages
------
1. ``install_source(config)`` validates and immutably installs one source
   (``managed-cdc-source/v1``).  A source exclusively owns its managed target
   tables, which must be empty and unclaimed at registration time.
2. ``receive_file(path)`` / ``receive_fragments(items)`` only *stage*
   fragments.  A delivery is all-or-nothing: a trailing bad JSON line or any
   semantic conflict rejects the whole file and writes nothing.  Staging never
   touches managed data, the source consumption cursor or the normal ledger.
3. ``source_status(source_id)`` explains why each sequence is waiting:
   missing transaction / missing begin / missing end / missing changes /
   ready / consumed / rejected-pending / skipped.
4. ``advance(source_id)`` processes the contiguous ready prefix.  Each
   committed source transaction becomes exactly one ledger batch with original
   source evidence; generation, decision, identity mapping and cursor commit
   in one SQLite transaction.  A business rejection records a durable decision,
   rolls every effect back and stops the cursor; nothing later can overtake it.
5. ``skip_rejected(source_id, tx_seq, reason, decision_digest)`` explicitly
   bypasses the one blocking rejected transaction, bound to its recorded
   decision summary.

Identity allocation is persistent, deterministic and collision-free: source
composite keys are stored as typed canonical tuples (int 1, bool true and text
"1" stay three identities), mapped to int64 managed row ids via per-table high
water marks, and mappings survive deletes so a recreated key reuses its row id.
"""

import json
import os

from .canon import canon, content_id, sha256
from .cdcp import (CDC_CONFIG_SCHEMA, CDC_PROGRAM_SCHEMA,
                   INT64_MAX, check_fragment_against_config,
                   validate_fragment, validate_source_config,
                   validate_cdc_program)
from .cdcexec import execute_cdc_program
from .errors import LedgerError, ProtocolError
from .ledger import _batch_components
from .store import utc_now

# Status vocabulary returned by source_status / explain_transaction.
ST_MISSING_TX = "missing_transaction"
ST_MISSING_BEGIN = "missing_begin"
ST_MISSING_END = "missing_end"
ST_MISSING_CHANGES = "missing_changes"
ST_READY = "ready"
ST_CONSUMED_COMMITTED = "consumed_committed"
ST_CONSUMED_ABORTED = "consumed_aborted"
ST_REJECTED_PENDING = "rejected_pending"
ST_SKIPPED = "skipped"


def key_tuple_json(row, key_fields):
    """Typed, boundary-preserving encoding of a source composite key.

    Each component keeps its JSON type, so integer 1, boolean true and text
    "1" produce three distinct persisted identities.  The tuple is an ordered
    JSON array (never a concatenated string), so delimiter/type boundaries are
    preserved.
    """
    return canon([row[f] for f in key_fields])


class CdcBusyError(LedgerError):
    """Another process currently holds the source's advance lock."""


class CdcIngestor:
    """Public entry point for configuration, reception and advancement."""

    def __init__(self, ledger):
        self.ledger = ledger
        self.db = ledger.db
        self.db.ensure_cdc_schema()
        # Crash injection points used by the subprocess crash drill.  Names map
        # to hard exits around the critical sections; production code never
        # sets them.
        self.crash_points = {}

    # =========================================================== utilities
    def _source_row(self, source_id):
        r = self.db.conn.execute(
            "SELECT * FROM cdc_source WHERE source_id=?", (source_id,)).fetchone()
        if r is None:
            return None
        return {"source_id": r["source_id"], "start_seq": r["start_seq"],
                "config": json.loads(r["config_json"]),
                "config_hash": r["config_hash"],
                "installed_at": r["installed_at"],
                "installed_at_generation": r["installed_at_generation"]}

    def _require_source(self, source_id):
        row = self._source_row(source_id)
        if row is None:
            raise LedgerError(f"unknown CDC source {source_id!r}")
        return row

    def _tables_cfg(self, config):
        return {tc["source_table"]: tc for tc in config["tables"]}

    # =========================================================== config
    def install_source(self, config):
        """Validate and immutably install a source configuration.

        The whole configuration is validated before any row is written, so an
        invalid install leaves the database exactly as it was.
        """
        normalized = validate_source_config(config)
        source_id = normalized["source_id"]
        existing = self._source_row(source_id)
        if existing is not None:
            if existing["config_hash"] == sha256(normalized):
                return self.source_view(source_id)
            raise LedgerError(
                f"CDC source {source_id!r} is already installed with different "
                f"rules; interpretation rules are immutable")

        # Another source may not claim the same managed table.
        claimed = {r["managed_table"] for r in self.db.conn.execute(
            "SELECT managed_table FROM cdc_source_table")}
        head = self.ledger.head()
        head_state = self.db.state(head["generation"])
        for tc in normalized["tables"]:
            mng = tc["managed_table"]
            if mng in claimed:
                raise LedgerError(
                    f"managed table {mng!r} is already owned by another CDC "
                    f"source; one source exclusively owns its target tables")
            # Target tables must be empty (rows) at registration.
            if head_state.row_ids(mng):
                raise LedgerError(
                    f"managed table {mng!r} must be empty when source "
                    f"{source_id!r} is registered")
            # Ordinary ledger programs must not already have a footprint there.
            used = self.db.conn.execute(
                "SELECT 1 FROM ledger_batch WHERE write_footprint_json LIKE ? "
                "LIMIT 1", (f'%"{mng}"%',)).fetchone()
            if used is not None:
                raise LedgerError(
                    f"managed table {mng!r} already appears in an existing "
                    f"ledger batch; a CDC source needs exclusive ownership")

        cfg_hash = sha256(normalized)
        with self.db.tx():
            # Re-check inside the transaction to close the registration race.
            claimed2 = {r["managed_table"] for r in self.db.conn.execute(
                "SELECT managed_table FROM cdc_source_table")}
            for tc in normalized["tables"]:
                if tc["managed_table"] in claimed2:
                    raise LedgerError(
                        f"managed table {tc['managed_table']!r} was claimed by "
                        f"another source concurrently")
            self.db.conn.execute(
                "INSERT INTO cdc_source(source_id,start_seq,config_json,"
                "config_hash,installed_at,installed_at_generation) "
                "VALUES(?,?,?,?,?,?)",
                (source_id, normalized["start_seq"], canon(normalized), cfg_hash,
                 utc_now(), head["generation"]))
            for ord_, tc in enumerate(normalized["tables"]):
                self.db.conn.execute(
                    "INSERT INTO cdc_source_table(source_id,source_table,"
                    "managed_table,key_fields,ord) VALUES(?,?,?,?,?)",
                    (source_id, tc["source_table"], tc["managed_table"],
                     canon(tc["key_fields"]), ord_))
                for ro, ref in enumerate(tc["references"]):
                    self.db.conn.execute(
                        "INSERT INTO cdc_source_ref(source_id,source_table,"
                        "column_name,ref_table,ord) VALUES(?,?,?,?,?)",
                        (source_id, tc["source_table"], ref["column"],
                         ref["references"], ro))
            self.db.conn.execute(
                "INSERT INTO cdc_cursor(source_id,next_seq,updated_at) "
                "VALUES(?,?,?)",
                (source_id, normalized["start_seq"], utc_now()))
            for tc in normalized["tables"]:
                self.db.conn.execute(
                    "INSERT INTO cdc_allocator(source_id,managed_table,"
                    "high_water) VALUES(?,?,0)",
                    (source_id, tc["managed_table"]))
        return self.source_view(source_id)

    def source_view(self, source_id):
        """Persistent read-back of the immutable configuration."""
        row = self._require_source(source_id)
        tables = []
        for tr in self.db.conn.execute(
                "SELECT * FROM cdc_source_table WHERE source_id=? ORDER BY ord",
                (source_id,)):
            refs = [{"column": r["column_name"], "references": r["ref_table"]}
                    for r in self.db.conn.execute(
                        "SELECT * FROM cdc_source_ref WHERE source_id=? AND "
                        "source_table=? ORDER BY ord",
                        (source_id, tr["source_table"]))]
            tables.append({"source_table": tr["source_table"],
                           "managed_table": tr["managed_table"],
                           "key_fields": json.loads(tr["key_fields"]),
                           "references": refs})
        return {"schema": CDC_CONFIG_SCHEMA, "source_id": source_id,
                "start_seq": row["start_seq"], "config_hash": row["config_hash"],
                "installed_at": row["installed_at"],
                "installed_at_generation": row["installed_at_generation"],
                "tables": tables}

    def sources(self):
        return [r["source_id"] for r in self.db.conn.execute(
            "SELECT source_id FROM cdc_source ORDER BY source_id")]

    # =========================================================== reception
    @staticmethod
    def _fragment_identity(frag):
        """Stable per-transaction identity of one fragment.

        begin/end are identified by their part; a change by its index.
        """
        if frag["part"] == "change":
            return (frag["part"], frag["index"])
        return (frag["part"], None)

    @staticmethod
    def _semantic_hash(frag):
        # Content hashing over canonical JSON: object key order is irrelevant,
        # while array order, scalar types and values are preserved.
        return sha256(frag)

    def _existing_fragment(self, source_id, tx_seq, identity):
        part, index = identity
        if part == "change":
            r = self.db.conn.execute(
                "SELECT * FROM cdc_fragment WHERE source_id=? AND tx_seq=? "
                "AND part='change' AND change_index=?",
                (source_id, tx_seq, index)).fetchone()
        else:
            r = self.db.conn.execute(
                "SELECT * FROM cdc_fragment WHERE source_id=? AND tx_seq=? "
                "AND part=? AND change_index IS NULL",
                (source_id, tx_seq, part)).fetchone()
        return r

    def _check_reception(self, frag, config):
        """All validation that can be decided without knowing tx assembly."""
        if frag["source_id"] != config["source_id"]:
            raise ProtocolError(
                f"fragment source_id {frag['source_id']!r} does not match the "
                f"addressed source {config['source_id']!r}")
        if frag["tx_seq"] < config["start_seq"]:
            raise ProtocolError(
                f"fragment tx_seq {frag['tx_seq']} precedes the configured start "
                f"sequence {config['start_seq']}")
        check_fragment_against_config(frag, config)

    def _validate_delivery_items(self, items, config):
        """Parse/validate every item up front (all-or-nothing delivery).

        Returns a list of (fragment, identity, semantic_hash).  Raises on the
        first invalid item, before any SQLite write.  Every fragment must
        address the delivery's source; a single delivery cannot mix sources.
        """
        parsed = []
        for n, item in enumerate(items):
            if not isinstance(item, dict):
                raise ProtocolError(
                    f"delivery item {n}: fragment must be a JSON object")
            frag = validate_fragment(item)
            if frag["source_id"] != config["source_id"]:
                raise ProtocolError(
                    f"delivery item {n}: fragment source_id "
                    f"{frag['source_id']!r} does not match the addressed "
                    f"source {config['source_id']!r}; one delivery belongs to "
                    f"one source")
            self._check_reception(frag, config)
            parsed.append((frag, self._fragment_identity(frag),
                           self._semantic_hash(frag)))
        return parsed

    def receive_fragments(self, items, delivery_id=None):
        """Stage one in-memory list of fragments; all succeed or none write."""
        if not isinstance(items, list) or not items:
            raise ProtocolError("a delivery must be a non-empty list of fragments")
        first = items[0] if isinstance(items[0], dict) else {}
        source_id = first.get("source_id")
        if not isinstance(source_id, str) or not source_id:
            raise ProtocolError("delivery fragments must carry a source_id")
        config_row = self._require_source(source_id)
        config = config_row["config"]
        parsed = self._validate_delivery_items(items, config)

        delivery_digest = sha256([p[0] for p in parsed])
        if delivery_id is not None:
            prior = self.db.conn.execute(
                "SELECT * FROM cdc_delivery WHERE delivery_id=?",
                (delivery_id,)).fetchone()
            if prior is not None:
                if prior["digest"] != delivery_digest:
                    raise LedgerError(
                        f"delivery id {delivery_id!r} was already used with "
                        f"different content")
                # Exact redelivery: return the original reception result; no
                # fragment is re-staged or re-validated for effect.
                return json.loads(prior["result_json"])

        results = []
        with self.db.tx():
            for frag, identity, sem_hash in parsed:
                results.append(self._stage_one_locked(frag, identity, sem_hash))
            did = delivery_id or content_id(
                "cdcdel", {"source": source_id, "digest": delivery_digest,
                           "at": utc_now()})
            summary = {"delivery_id": did, "source_id": source_id,
                       "fragment_count": len(parsed),
                       "staged": sum(1 for r in results if r["status"] == "staged"),
                       "duplicate": sum(1 for r in results
                                        if r["status"] == "duplicate"),
                       "note": "fragments are staged only; nothing has been "
                               "loaded into managed tables"}
            self.db.conn.execute(
                "INSERT INTO cdc_delivery(delivery_id,source_id,fragment_count,"
                "digest,result_json,delivered_at) VALUES(?,?,?,?,?,?)",
                (did, source_id, len(parsed), delivery_digest, canon(summary),
                 utc_now()))
        summary["fragments"] = results
        return summary

    def _stage_one_locked(self, frag, identity, sem_hash):
        """Insert/verify one fragment inside the delivery transaction."""
        source_id, tx_seq, tx_id = (frag["source_id"], frag["tx_seq"],
                                    frag["tx_id"])
        # Sequence <-> tx_id is a strict one-to-one binding (its own table,
        # since one transaction owns many fragment rows).
        binding = self.db.conn.execute(
            "SELECT * FROM cdc_tx_binding WHERE source_id=? AND tx_seq=?",
            (source_id, tx_seq)).fetchone()
        if binding is None:
            clash = self.db.conn.execute(
                "SELECT tx_seq FROM cdc_tx_binding WHERE source_id=? AND tx_id=?",
                (source_id, tx_id)).fetchone()
            if clash is not None:
                raise ProtocolError(
                    f"tx binding conflict: tx_id {tx_id!r} is already bound to "
                    f"seq {clash['tx_seq']}, cannot reuse it for seq {tx_seq}")
            self.db.conn.execute(
                "INSERT INTO cdc_tx_binding(source_id,tx_seq,tx_id) "
                "VALUES(?,?,?)", (source_id, tx_seq, tx_id))
        elif binding["tx_id"] != tx_id:
            raise ProtocolError(
                f"tx binding conflict: seq {tx_seq} is already bound to tx_id "
                f"{binding['tx_id']!r}, cannot rebind it to {tx_id!r}")
        existing = self._existing_fragment(source_id, tx_seq, identity)
        if existing is not None:
            if existing["semantic_hash"] != sem_hash:
                raise ProtocolError(
                    f"conflicting fragment for source {source_id!r} tx_seq "
                    f"{tx_seq} identity {identity!r}: same identity, different "
                    f"semantic content")
            return {"tx_seq": tx_seq, "tx_id": tx_id, "part": frag["part"],
                    "change_index": frag.get("index"),
                    "status": "duplicate",
                    "note": "identical semantic content; no new effect"}

        # If an end is already staged, its change_count must cover this index.
        end = self.db.conn.execute(
            "SELECT fragment_json FROM cdc_fragment WHERE source_id=? AND "
            "tx_seq=? AND part='end'", (source_id, tx_seq)).fetchone()
        if end is not None and frag["part"] == "change":
            count = json.loads(end["fragment_json"])["change_count"]
            if frag["index"] > count:
                raise ProtocolError(
                    f"change index {frag['index']} exceeds the staged end "
                    f"change_count {count} for tx_seq {tx_seq}")
        if frag["part"] == "end":
            for ch in self.db.conn.execute(
                    "SELECT change_index FROM cdc_fragment WHERE source_id=? "
                    "AND tx_seq=? AND part='change'", (source_id, tx_seq)):
                if ch["change_index"] > frag["change_count"]:
                    raise ProtocolError(
                        f"end change_count {frag['change_count']} contradicts "
                        f"already staged change index {ch['change_index']}")

        idx = frag["index"] if frag["part"] == "change" else None
        self.db.conn.execute(
            "INSERT INTO cdc_fragment(source_id,tx_seq,tx_id,part,change_index,"
            "semantic_hash,fragment_json,received_at) VALUES(?,?,?,?,?,?,?,?)",
            (source_id, tx_seq, tx_id, frag["part"], idx, sem_hash,
             canon(frag), utc_now()))
        return {"tx_seq": tx_seq, "tx_id": tx_id, "part": frag["part"],
                "change_index": idx, "status": "staged"}

    def receive_file(self, path, delivery_id=None):
        """Read a JSONL file and stage it atomically.

        Every line must be a complete, parseable JSON object; a truncated or
        invalid trailing line rejects the whole file, so earlier valid lines
        are never partially accepted.
        """
        items = []
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
        for ln, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                raise ProtocolError(
                    f"{path}: line {ln}: blank lines are not valid fragments")
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ProtocolError(
                    f"{path}: line {ln}: malformed JSON ({exc.msg}); the whole "
                    f"delivery is rejected and nothing is written") from exc
            items.append(obj)
        if not items:
            raise ProtocolError(f"{path}: empty fragment file")
        did = delivery_id or content_id(
            "cdcfile", {"path": os.path.basename(path),
                        "digest": sha256(text)})
        return self.receive_fragments(items, delivery_id=did)

    # =========================================================== status
    def _cursor(self, source_id):
        r = self.db.conn.execute(
            "SELECT * FROM cdc_cursor WHERE source_id=?", (source_id,)).fetchone()
        return None if r is None else r["next_seq"]

    def _decision(self, source_id, tx_seq):
        r = self.db.conn.execute(
            "SELECT * FROM cdc_tx_decision WHERE source_id=? AND tx_seq=?",
            (source_id, tx_seq)).fetchone()
        if r is None:
            return None
        return {"source_id": source_id, "tx_seq": tx_seq, "tx_id": r["tx_id"],
                "outcome": r["outcome"], "decision_digest": r["decision_digest"],
                "summary": json.loads(r["summary_json"]),
                "detail": json.loads(r["detail_json"]),
                "decided_at": r["decided_at"]}

    def _fragments_of(self, source_id, tx_seq):
        rows = self.db.conn.execute(
            "SELECT * FROM cdc_fragment WHERE source_id=? AND tx_seq=?",
            (source_id, tx_seq)).fetchall()
        return [json.loads(r["fragment_json"]) for r in rows]

    def _tx_state(self, source_id, tx_seq):
        """Classify one sequence from staged fragments and decisions."""
        decision = self._decision(source_id, tx_seq)
        skip = self.db.conn.execute(
            "SELECT 1 FROM cdc_skip WHERE source_id=? AND tx_seq=?",
            (source_id, tx_seq)).fetchone()
        if decision is not None and skip is not None:
            return ST_SKIPPED, decision
        if decision is not None:
            if decision["outcome"] == "committed":
                return ST_CONSUMED_COMMITTED, decision
            if decision["outcome"] == "aborted":
                return ST_CONSUMED_ABORTED, decision
            return ST_REJECTED_PENDING, decision

        rows = self.db.conn.execute(
            "SELECT * FROM cdc_fragment WHERE source_id=? AND tx_seq=?",
            (source_id, tx_seq)).fetchall()
        if not rows:
            return ST_MISSING_TX, None
        parts = {r["part"]: r for r in rows if r["part"] != "change"}
        changes = sorted(r["change_index"] for r in rows if r["part"] == "change")
        if "begin" not in parts:
            return ST_MISSING_BEGIN, None
        if "end" not in parts:
            return ST_MISSING_END, None
        count = json.loads(parts["end"]["fragment_json"])["change_count"]
        if changes != list(range(1, count + 1)):
            missing = [i for i in range(1, count + 1) if i not in set(changes)]
            return ST_MISSING_CHANGES, {"missing_change_indexes": missing,
                                        "change_count": count}
        return ST_READY, None

    def source_status(self, source_id, window=None):
        """Explain waiting reasons across the source's sequence window."""
        config_row = self._require_source(source_id)
        start = config_row["start_seq"]
        next_seq = self._cursor(source_id)
        seqs = {r["tx_seq"] for r in self.db.conn.execute(
            "SELECT DISTINCT tx_seq FROM cdc_fragment WHERE source_id=?",
            (source_id,))}
        seqs.update(r["tx_seq"] for r in self.db.conn.execute(
            "SELECT tx_seq FROM cdc_tx_decision WHERE source_id=?",
            (source_id,)))
        upper = max(seqs, default=next_seq - 1)
        upper = max(upper, start)
        if window is not None:
            upper = max(upper, start + window - 1)
        transactions = []
        first_blocker = None
        for seq in range(start, upper + 1):
            state, decision = self._tx_state(source_id, seq)
            entry = {"tx_seq": seq, "state": state}
            if state == ST_MISSING_CHANGES:
                # Surface exactly which change indexes are still absent.
                rows = self.db.conn.execute(
                    "SELECT change_index FROM cdc_fragment WHERE source_id=? "
                    "AND tx_seq=? AND part='change'", (source_id, seq)).fetchall()
                have = {r["change_index"] for r in rows}
                end_row = self.db.conn.execute(
                    "SELECT fragment_json FROM cdc_fragment WHERE source_id=? "
                    "AND tx_seq=? AND part='end'", (source_id, seq)).fetchone()
                count = json.loads(end_row["fragment_json"])["change_count"]
                entry["missing_change_indexes"] = [
                    i for i in range(1, count + 1) if i not in have]
                entry["change_count"] = count
            if decision is not None and state == ST_REJECTED_PENDING:
                entry["decision_digest"] = decision["decision_digest"]
                entry["summary"] = decision["summary"]
            transactions.append(entry)
            if first_blocker is None and state in (
                    ST_MISSING_TX, ST_MISSING_BEGIN, ST_MISSING_END,
                    ST_MISSING_CHANGES, ST_REJECTED_PENDING):
                first_blocker = entry
        return {"source_id": source_id, "start_seq": start,
                "next_seq": next_seq,
                "consumed_through": next_seq - 1,
                "first_blocker": first_blocker,
                "transactions": transactions,
                "note": "staged fragments are not managed writes; only a "
                        "successful advance moves the cursor and the head"}

    def explain_transaction(self, source_id, tx_seq):
        """Full read-back: fragments, assembly state and the durable decision."""
        self._require_source(source_id)
        state, decision = self._tx_state(source_id, tx_seq)
        out = {"source_id": source_id, "tx_seq": tx_seq, "state": state,
               "fragments": self._fragments_of(source_id, tx_seq)}
        if decision is not None:
            out["decision"] = decision
            link = self.db.conn.execute(
                "SELECT * FROM cdc_batch_link WHERE source_id=? AND tx_seq=?",
                (source_id, tx_seq)).fetchone()
            if link is not None:
                out["batch"] = {"batch_ord": link["batch_ord"],
                                "logical_batch_id": link["logical_batch_id"]}
            out["change_maps"] = [
                {"change_index": r["change_index"], "op": r["op"],
                 "source_table": r["source_table"],
                 "managed_table": r["managed_table"],
                 "source_key": json.loads(r["key_json"]),
                 "row_id": r["row_id"]}
                for r in self.db.conn.execute(
                    "SELECT * FROM cdc_change_map WHERE source_id=? AND "
                    "tx_seq=? ORDER BY change_index", (source_id, tx_seq))]
            skip = self.db.conn.execute(
                "SELECT * FROM cdc_skip WHERE source_id=? AND tx_seq=?",
                (source_id, tx_seq)).fetchone()
            if skip is not None:
                out["skip"] = {"reason": skip["reason"],
                               "decision_digest": skip["decision_digest"],
                               "skipped_at": skip["skipped_at"]}
        return out

    def explain_row(self, managed_table, row_id):
        """Reverse lookup: managed row -> source coordinates and key mapping."""
        r = self.db.conn.execute(
            "SELECT * FROM cdc_identity WHERE managed_table=? AND row_id=?",
            (managed_table, row_id)).fetchone()
        if r is None:
            raise LedgerError(
                f"no CDC identity mapping for {managed_table}[{row_id}]")
        return {"source_id": r["source_id"], "source_table": r["source_table"],
                "managed_table": r["managed_table"], "row_id": r["row_id"],
                "source_key": json.loads(r["key_json"]),
                "created_tx_seq": r["created_tx_seq"]}

    def explain_batch_cdc(self, batch_ord):
        """Source coordinates attached to a ledger batch (if CDC-produced)."""
        r = self.db.conn.execute(
            "SELECT * FROM cdc_batch_link WHERE batch_ord=?",
            (batch_ord,)).fetchone()
        if r is None:
            return None
        return {"source_id": r["source_id"], "tx_seq": r["tx_seq"],
                "tx_id": r["tx_id"], "batch_ord": r["batch_ord"],
                "logical_batch_id": r["logical_batch_id"],
                "change_maps": [
                    {"change_index": m["change_index"], "op": m["op"],
                     "source_table": m["source_table"],
                     "managed_table": m["managed_table"],
                     "source_key": json.loads(m["key_json"]),
                     "row_id": m["row_id"]}
                    for m in self.db.conn.execute(
                        "SELECT * FROM cdc_change_map WHERE source_id=? AND "
                        "tx_seq=? ORDER BY change_index",
                        (r["source_id"], r["tx_seq"]))]}

    # =========================================================== assembly
    def _assembled(self, source_id, tx_seq):
        """Return the ordered fragments of a complete, ready transaction."""
        state, _ = self._tx_state(source_id, tx_seq)
        if state != ST_READY:
            return None
        rows = {r["part"]: r for r in self.db.conn.execute(
            "SELECT * FROM cdc_fragment WHERE source_id=? AND tx_seq=? "
            "AND part != 'change'", (source_id, tx_seq))}
        begin = json.loads(rows["begin"]["fragment_json"])
        end = json.loads(rows["end"]["fragment_json"])
        changes = [json.loads(r["fragment_json"]) for r in self.db.conn.execute(
            "SELECT fragment_json FROM cdc_fragment WHERE source_id=? AND "
            "tx_seq=? AND part='change' ORDER BY change_index",
            (source_id, tx_seq))]
        return begin, end, changes

    def _convert_row(self, row, tc, table_cfgs, mapping_plan):
        """Convert a source row to its managed representation.

        Non-reference columns keep name/type/value verbatim.  A declared
        reference column is either null or a source key array that resolves
        through the transaction's pre-registered identity plan to the parent's
        managed row id.  Resolution only translates an identity; it never
        asserts the parent row exists -- final existence is the constraint
        gate's job.
        """
        out = {}
        ref_cols = {r["column"]: r for r in tc["references"]}
        for col, val in row.items():
            if col in ref_cols:
                if val is None:
                    out[col] = None
                    continue
                target_table = ref_cols[col]["references"]
                target_cfg = table_cfgs[target_table]
                key_json = canon(val)
                rid = mapping_plan[(target_table, key_json)]
                out[col] = rid
            else:
                out[col] = val
        return out

    def _build_program(self, source_id, tx_seq, tx_id, end, changes, config):
        """Pure preparation of a managed-cdc/v1 program and its allocations.

        All identities the transaction needs are registered *before* any row
        is converted, so child rows can arrive before parents and mutual
        references resolve.  Allocation order is deterministic (sorted table
        and typed key), independent of fragment arrival order.  No SQLite
        writes happen here: the plan is committed together with the batch or
        discarded wholesale on rejection.
        """
        table_cfgs = self._tables_cfg(config)
        existing_maps = {}     # (source_table, key_json) -> row_id
        for r in self.db.conn.execute(
                "SELECT source_table,key_json,row_id FROM cdc_identity "
                "WHERE source_id=?", (source_id,)):
            existing_maps[(r["source_table"], r["key_json"])] = r["row_id"]
        waters = {r["managed_table"]: r["high_water"] for r in
                  self.db.conn.execute(
                      "SELECT managed_table,high_water FROM cdc_allocator "
                      "WHERE source_id=?", (source_id,))}
        managed_of = {tc["source_table"]: tc["managed_table"]
                      for tc in config["tables"]}

        # ---- phase 1: collect every key touched, and every referenced key ---
        needed = set()          # (source_table, key_json)

        def key_of(frag_row, table_name):
            tc = table_cfgs[table_name]
            return key_tuple_json(frag_row, tc["key_fields"])

        for ch in changes:
            tc = table_cfgs[ch["table"]]
            row = ch["after"] if ch["after"] is not None else ch["before"]
            needed.add((ch["table"], key_of(row, ch["table"])))
            for ref in tc["references"]:
                if row is not None and ref["column"] in row and \
                        row[ref["column"]] is not None:
                    arr = row[ref["column"]]
                    needed.add((ref["references"], canon(arr)))

        # ---- phase 2: deterministic identity registration ------------------
        new_allocations = []
        full_mapping = dict(existing_maps)
        # A test seam (defaulting to the real int64 ceiling) lets capacity
        # exhaustion be exercised without allocating 2^63 identities.
        limit = getattr(self, "row_id_limit", INT64_MAX)
        for source_table, key_json in sorted(needed):
            if (source_table, key_json) in full_mapping:
                continue
            mng = managed_of[source_table]
            if waters[mng] >= limit:
                raise LedgerError(
                    f"row_id capacity exhausted for {mng!r} of source "
                    f"{source_id!r}; the transaction is refused in full")
            waters[mng] += 1
            rid = waters[mng]
            full_mapping[(source_table, key_json)] = rid
            new_allocations.append(
                {"source_table": source_table, "managed_table": mng,
                 "key_json": key_json, "row_id": rid})

        # ---- phase 3: convert rows and build ordered changes ---------------
        prog_changes, change_maps = [], []
        for ch in changes:
            tc = table_cfgs[ch["table"]]
            mng = tc["managed_table"]
            key_json = key_of(
                ch["after"] if ch["after"] is not None else ch["before"],
                ch["table"])
            rid = full_mapping[(ch["table"], key_json)]
            before = None if ch["before"] is None else self._convert_row(
                ch["before"], tc, table_cfgs, full_mapping)
            after = None if ch["after"] is None else self._convert_row(
                ch["after"], tc, table_cfgs, full_mapping)
            prog_changes.append(
                {"index": ch["index"], "op": ch["op"],
                 "source_table": ch["table"], "managed_table": mng,
                 "target_row_id": rid, "key_json": key_json,
                 "before": before, "after": after})
            change_maps.append(
                {"change_index": ch["index"], "op": ch["op"],
                 "source_table": ch["table"], "managed_table": mng,
                 "key_json": key_json, "row_id": rid})

        logical_id = f"cdc:{source_id}:{tx_seq}:{tx_id}"
        # Allocations are ordered for deterministic program bytes.
        new_allocations = sorted(
            new_allocations, key=lambda a: (a["managed_table"], a["row_id"]))
        program = {"logical_batch_id": logical_id,
                   "schema": CDC_PROGRAM_SCHEMA, "source_id": source_id,
                   "tx_seq": tx_seq, "tx_id": tx_id,
                   "allocations": new_allocations,
                   "changes": prog_changes}
        validate_cdc_program(program)
        return program, new_allocations, change_maps, logical_id

    # =========================================================== advance
    def _ordinary_pending(self):
        """Normal ingest rows that have no ledger decision yet."""
        decided = {b["batch_ord"] for b in self.db.batches()}
        return [b for b in self.db.ingest_batches()
                if b["batch_ord"] not in decided]

    def advance(self, source_id, limit=None):
        """Process the contiguous ready prefix of one source.

        Idempotent: already-consumed sequences are reported from their durable
        decisions and never re-executed.  A business rejection stops the run;
        the same content advanced again returns the same rejection without an
        automatic retry.
        """
        config_row = self._require_source(source_id)
        config = config_row["config"]
        head = self.ledger.head()
        if self.db.generation_info(head["generation"])["kind"] == "revocation":
            raise LedgerError(
                "the committed head is a revocation generation; CDC advancement "
                "is closed in that lineage (evidence remains readable)")
        pending = self._ordinary_pending()
        if pending:
            raise LedgerError(
                f"blocked: {len(pending)} ordinary ledger batch(es) "
                f"{[b['batch_ord'] for b in pending]} are ingested but not yet "
                f"decided; the CDC ingestor will not overtake them or create a "
                f"dangling batch")

        # Serialize concurrent advancers across processes.  A busy database is
        # reported cleanly; the loser retries and reads the winner's decision.
        import sqlite3
        self.db.conn.execute("PRAGMA busy_timeout=750")
        try:
            self.db.conn.execute("BEGIN IMMEDIATE")
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
            self.db.conn.execute("PRAGMA busy_timeout=30000")
            raise CdcBusyError(
                f"source {source_id!r} is being advanced by another process") from exc
        crash_after_commit = False
        try:
            result = self._advance_locked(source_id, config, limit)
            crash_after_commit = self.crash_points.get(
                "after_commit_before_response", False)
            # The cursor update happens inside the same held transaction, so
            # generation, ledger decision, mappings and cursor become durable
            # together.
            self.db.conn.execute("COMMIT")
        except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                self.db.conn.execute("ROLLBACK")
                self.db.conn.execute("PRAGMA busy_timeout=30000")
                raise CdcBusyError(
                    f"source {source_id!r} is being advanced by another "
                    f"process") from exc
            self.db.conn.execute("ROLLBACK")
            raise
        except Exception:
            self.db.conn.execute("ROLLBACK")
            raise
        finally:
            self.db.conn.execute("PRAGMA busy_timeout=30000")
        if crash_after_commit:
            # The commit is durable; only the caller's response is lost.
            self._hard_exit("after_commit_before_response")
        return result

    def _advance_locked(self, source_id, config, limit):
        decisions = []
        next_seq = self._cursor(source_id)
        processed = 0
        crash_after_staging = self.crash_points.get(
            "after_staging_before_commit", False)
        while True:
            seq = next_seq
            state, prior = self._tx_state(source_id, seq)
            if state in (ST_CONSUMED_COMMITTED, ST_CONSUMED_ABORTED, ST_SKIPPED):
                # Defensive: cursor should already have passed these.
                decisions.append({"tx_seq": seq, "outcome": prior["outcome"],
                                  "already_decided": True})
                next_seq += 1
                continue
            if state == ST_REJECTED_PENDING:
                decisions.append({"tx_seq": seq, "outcome": "rejected",
                                  "already_decided": True,
                                  "decision_digest": prior["decision_digest"],
                                  "summary": prior["summary"]})
                break
            if state != ST_READY:
                break
            if limit is not None and processed >= limit:
                break

            assembled = self._assembled(source_id, seq)
            begin, end, changes = assembled
            tx_id = begin["tx_id"]
            decision = self._decide_one(
                source_id, config, seq, tx_id, end, changes)
            decisions.append(decision)
            processed += 1
            if decision["outcome"] in ("committed", "aborted", "skipped"):
                next_seq = seq + 1
                # Crash point: the fragments were already durable and this
                # transaction's commit work is staged, but nothing has been
                # committed yet (the outer transaction stays open).  Reopening
                # must show no half transaction and an unchanged cursor.
                if crash_after_staging and decision["outcome"] == "committed":
                    self._hard_exit("after_staging_before_commit")
                continue
            # rejected: cursor stays, later sequences cannot overtake.
            break

        if next_seq != self._cursor(source_id):
            self.db.conn.execute(
                "UPDATE cdc_cursor SET next_seq=?, updated_at=? WHERE source_id=?",
                (next_seq, utc_now(), source_id))
        elif not decisions:
            # Response-loss / no-op polling: return the last durable decision
            # at the cursor's predecessor so retries get the same answer
            # without re-executing anything.
            prev = self._decision(source_id, next_seq - 1)
            if prev is not None:
                decisions.append({"tx_seq": prev["tx_seq"],
                                  "outcome": prev["outcome"],
                                  "already_decided": True,
                                  "decision_digest": prev["decision_digest"],
                                  "summary": prev["summary"]})
        return {"source_id": source_id, "next_seq": next_seq,
                "consumed_through": next_seq - 1, "decisions": decisions,
                "note": "only complete contiguous transactions were processed; "
                        "staging alone never moved this cursor"}

    def _decide_one(self, source_id, config, seq, tx_id, end, changes):
        """Prepare, execute and durably record exactly one source transaction.

        Runs inside the advancer's held transaction (BEGIN IMMEDIATE).  On a
        business rejection every managed effect, identity and allocator change
        is rolled back to a savepoint; only the rejection decision survives.
        """
        # Savepoint so a rejected commit cannot leak mappings/allocator moves.
        self.db.conn.execute("SAVEPOINT cdc_tx")
        try:
            try:
                program, allocations, change_maps, logical_id = \
                    self._build_program(
                        source_id, seq, tx_id, end, changes, config)
            except LedgerError as exc:
                # Preparation failures (e.g. row_id capacity exhausted) are
                # business rejections: roll every provisional effect back and
                # record a queryable decision like an execution rejection.
                self.db.conn.execute("ROLLBACK TO SAVEPOINT cdc_tx")
                self.db.conn.execute("RELEASE SAVEPOINT cdc_tx")
                abort = {"code": "cdc_capacity_exhausted",
                         "message": str(exc), "step_index": None,
                         "table": None, "row_id": None, "detail": {}}
                return self._record_rejection(
                    source_id, seq, tx_id, end, changes, abort)
            head = self.ledger.head()
            parent_gen = head["generation"]
            working = self.db.state(parent_gen)
            cs = self.ledger.constraints()

            if end["outcome"] == "abort":
                # A complete abort is consumed as a decision but has no row
                # effects and produces no ledger batch.
                self.db.conn.execute("RELEASE SAVEPOINT cdc_tx")
                return self._record_abort(source_id, seq, tx_id, end, changes,
                                          change_maps, allocations)

            evidence = execute_cdc_program(program, working, constraints=cs)
            if not evidence["ok"]:
                raise _CdcReject(evidence["abort"], evidence)

            phash = sha256(program)
            raw = {"logical_batch_id": logical_id, "program": program,
                   "batch_ord": None}
            batch_ord = self.db._insert_ingest_locked(logical_id, program, phash)
            raw["batch_ord"] = batch_ord
            trace = {"events": [{
                "event": "cdc_batch_commit",
                "source_id": source_id, "tx_seq": seq, "tx_id": tx_id,
                "components": _batch_components(
                    evidence, cs, b_ord=batch_ord,
                    logical_batch_id=logical_id)}]}
            gen = self.db._create_generation_locked(
                parent=parent_gen, kind="batch", cause="cdc", state=working,
                batch_ord=batch_ord, revocation_id=None, trace=trace)
            self.db.append_ledger(raw, evidence, {"ok": True}, None, phash,
                                  parent_gen, "success", gen["generation"],
                                  key_ranges=None)
            for a in allocations:
                self.db.conn.execute(
                    "INSERT INTO cdc_identity(source_id,source_table,key_json,"
                    "managed_table,row_id,created_tx_seq) VALUES(?,?,?,?,?,?)",
                    (source_id, a["source_table"], a["key_json"],
                     a["managed_table"], a["row_id"], seq))
                self.db.conn.execute(
                    "UPDATE cdc_allocator SET high_water=? WHERE source_id=? "
                    "AND managed_table=?",
                    (a["row_id"], source_id, a["managed_table"]))
            self.db.conn.execute(
                "INSERT INTO cdc_batch_link(source_id,tx_seq,tx_id,batch_ord,"
                "logical_batch_id) VALUES(?,?,?,?,?)",
                (source_id, seq, tx_id, batch_ord, logical_id))
            for m in change_maps:
                self.db.conn.execute(
                    "INSERT INTO cdc_change_map(source_id,tx_seq,change_index,"
                    "source_table,managed_table,op,key_json,row_id) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (source_id, seq, m["change_index"], m["source_table"],
                     m["managed_table"], m["op"], m["key_json"], m["row_id"]))
            summary = {"outcome": "committed", "tx_seq": seq, "tx_id": tx_id,
                       "batch_ord": batch_ord,
                       "logical_batch_id": logical_id,
                       "generation": gen["generation"],
                       "change_count": len(changes)}
            digest = sha256(summary)
            self.db.conn.execute(
                "INSERT INTO cdc_tx_decision(source_id,tx_seq,tx_id,outcome,"
                "decision_digest,summary_json,detail_json,decided_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (source_id, seq, tx_id, "committed", digest, canon(summary),
                 canon({"evidence": {"writes": evidence["writes"],
                                     "constraints": evidence["constraints"]},
                        "allocations": allocations}), utc_now()))
            self.db.conn.execute("RELEASE SAVEPOINT cdc_tx")
            return {"tx_seq": seq, "outcome": "committed",
                    "decision_digest": digest, "summary": summary}
        except _CdcReject as rej:
            self.db.conn.execute("ROLLBACK TO SAVEPOINT cdc_tx")
            self.db.conn.execute("RELEASE SAVEPOINT cdc_tx")
            return self._record_rejection(source_id, seq, tx_id, end, changes,
                                          rej.abort)
        except Exception:
            self.db.conn.execute("ROLLBACK TO SAVEPOINT cdc_tx")
            self.db.conn.execute("RELEASE SAVEPOINT cdc_tx")
            raise

    def _record_abort(self, source_id, seq, tx_id, end, changes, change_maps,
                      allocations):
        """Consume a complete source abort: decision only, no managed writes."""
        summary = {"outcome": "aborted", "tx_seq": seq, "tx_id": tx_id,
                   "change_count": len(changes),
                   "note": "source transaction aborted; row effects ignored"}
        digest = sha256(summary)
        self.db.conn.execute(
            "INSERT INTO cdc_tx_decision(source_id,tx_seq,tx_id,outcome,"
            "decision_digest,summary_json,detail_json,decided_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (source_id, seq, tx_id, "aborted", digest, canon(summary),
             canon({"ignored_change_count": len(changes),
                    "allocations_survive": False}), utc_now()))
        return {"tx_seq": seq, "outcome": "aborted",
                "decision_digest": digest, "summary": summary}

    def _record_rejection(self, source_id, seq, tx_id, end, changes, abort):
        """Persist a queryable rejection; all effects were rolled back."""
        summary = {"outcome": "rejected", "tx_seq": seq, "tx_id": tx_id,
                   "abort_code": abort.get("code"),
                   "abort_message": abort.get("message"),
                   "change_count": len(changes)}
        digest = sha256(summary)
        self.db.conn.execute(
            "INSERT INTO cdc_tx_decision(source_id,tx_seq,tx_id,outcome,"
            "decision_digest,summary_json,detail_json,decided_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (source_id, seq, tx_id, "rejected", digest, canon(summary),
             canon({"abort": abort}), utc_now()))
        return {"tx_seq": seq, "outcome": "rejected",
                "decision_digest": digest, "summary": summary,
                "abort": abort}

    # =========================================================== skip
    def skip_rejected(self, source_id, tx_seq, reason, decision_digest):
        """Explicitly skip the one blocking rejected transaction."""
        if not isinstance(reason, str) or not reason.strip():
            raise LedgerError("skip requires a non-empty reason")
        config_row = self._require_source(source_id)
        decision = self._decision(source_id, tx_seq)
        if decision is None:
            raise LedgerError(
                f"tx_seq {tx_seq} has no recorded decision; only a rejected "
                f"transaction can be skipped")
        if decision["outcome"] != "rejected":
            raise LedgerError(
                f"tx_seq {tx_seq} is {decision['outcome']!r}; only a rejected "
                f"transaction can be skipped")
        next_seq = self._cursor(source_id)
        if tx_seq != next_seq:
            raise LedgerError(
                f"tx_seq {tx_seq} is not the current blocking sequence "
                f"({next_seq}); only the rejected transaction actually blocking "
                f"the source can be skipped")
        if decision_digest != decision["decision_digest"]:
            raise LedgerError(
                "skip rejected: decision_digest does not match the recorded "
                "rejection summary; bind the skip to this exact decision")
        with self.db.tx():
            existing = self.db.conn.execute(
                "SELECT 1 FROM cdc_skip WHERE source_id=? AND tx_seq=?",
                (source_id, tx_seq)).fetchone()
            if existing is not None:
                raise LedgerError("this rejected transaction was already skipped")
            self.db.conn.execute(
                "INSERT INTO cdc_skip(source_id,tx_seq,reason,decision_digest,"
                "skipped_at) VALUES(?,?,?,?,?)",
                (source_id, tx_seq, reason, decision_digest, utc_now()))
            # The rejection record itself stays append-only; the skip row is
            # the durable skip decision and drives state classification.
            self.db.conn.execute(
                "UPDATE cdc_cursor SET next_seq=?, updated_at=? WHERE source_id=?",
                (tx_seq + 1, utc_now(), source_id))
        return self.explain_transaction(source_id, tx_seq)

    # =========================================================== crash drill
    def _hard_exit(self, point):
        # os._exit bypasses Python cleanup, modelling a real process kill.
        # Tests inject a raising callable instead so the surrounding
        # transaction rollback is observable in-process; the subprocess crash
        # drill leaves this at its real process-terminating default.
        hook = self.crash_points.get("_hard_exit_hook")
        if hook is not None:
            hook(point)
        os._exit(97)


class _CdcReject(Exception):
    """Internal control flow for a business rejection inside a savepoint."""

    def __init__(self, abort, evidence):
        self.abort = abort
        self.evidence = evidence
