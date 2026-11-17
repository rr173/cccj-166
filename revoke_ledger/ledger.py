"""Managed batch ledger: ingest, run, plan selective revocation, apply, recover.

Business rules
--------------
* Immutable time order: `ingest_record.batch_ord` is assigned once, in arrival
  order, and can never change.  A batch is executed only when all earlier
  ingests have a decision.
* A successful batch commits exactly one new generation atomically.  A failed
  batch records its evidence/decision and creates no generation and no writes.
* Revoking a successful batch starts from the committed state *before* that
  batch, does not run it, and re-evaluates the original programs of later
  successful batches in their original order.  Originally failed batches keep
  their historical decision and are never activated.
* A later batch is:
    Superseded  - the target itself (only one per plan);
    Kept        - its original result is still valid: either replayed and
                  produced identical reads and writes (`reused_identical`), or
                  provably unaffected and not executed (`independent_partition`);
    Changed     - replayed on the new inputs; produced a different result which
                  is the new official result;
    Blocked     - guard no longer holds or a write precondition (primary key /
                  source row) fails; it performs no writes and creates no
                  generation.
* The pre-target state is derived from generations; the old head and all
  original evidence are read-only.  The result is a new generation.
* One revocation id can be bound to exactly one target and applied at most once.
"""

import json

from .canon import canon, content_id, sha256
from .errors import BatchAbort, IntegrityError, LedgerError, StaleError
from .interpreter import (eval_id_expr, eval_pred, eval_value, execute_program,
                          normalize_field_value)
from .protocol import program_digest, program_reads, validate_program
from .store import state_promise, utc_now

REASON_TEXT = {
    "upstream_of_target": "batch precedes the revoked batch; it defines the pre-target state",
    "superseded_target": "revoked target; not executed, its written keys are removed by "
                         "deriving from the pre-target state",
    "independent_partition": "declared key partition is actually disjoint from the "
                             "revocation's changed keys; the batch was not executed and "
                             "its original output is retained verbatim",
    "reused_identical": "replayed on re-derived inputs; read results, write preconditions "
                        "and produced rows equal its original execution",
    "changed_replay": "replayed on re-derived inputs; produced a different result, which "
                      "is now the official result of this batch",
    "blocked_guard": "replayed on re-derived inputs; guard no longer holds, no writes",
    "blocked_pk_conflict": "replayed on re-derived inputs; a write precondition "
                           "(primary key/source row) failed, no writes",
    "blocked_step_failure": "replayed on re-derived inputs; a step failed, no writes",
    "blocked_historical_failure": "originally failed; historical decision preserved, not "
                                  "activated by revocation",
}


# ---------------------------------------------------------------------------
# small deterministic helpers
# ---------------------------------------------------------------------------
def _reads_env_from_evidence(batch):
    env = {}
    for r in batch["evidence"]["reads"]:
        rows = {c["row_id"]: json.loads(c["row"]) for c in r["classification"]
                if c["status"] == "selected"}
        env[r["alias"]] = {"table": r["table"], "rows_by_id": rows,
                           "selected_ids": r["selected_ids"]}
    return env


def _read_preconditions(state, read_step, orig):
    """Full read proof: every map identity's classification and values match."""
    table = read_step["select"]["table"]
    where = read_step["select"]["where"]
    classification = []
    for ent in read_step["map"]:
        rid = ent["row_id"]
        row = state.get_row(table, rid)
        if row is None:
            classification.append({"row_id": rid, "status": "absent"})
            continue
        keep = eval_pred(where, {"row": row, "row_id": rid, "reads": {}})
        classification.append({"row_id": rid,
                               "status": "selected" if keep else "excluded",
                               "row": canon(row)})
    same = classification == orig["classification"]
    return same, {
        "alias": read_step["alias"],
        "table": table,
        "map_entries_checked": len(read_step["map"]),
        "selected_count_original": orig["selected_count"],
        "selected_count_now": sum(1 for c in classification if c["status"] == "selected"),
        "classifications_match": same,
    }


def _write_preconditions(state, write_step, orig, reads_env):
    """Write targets' before-images and computed output bytes match the original."""
    table = write_step["table"]
    effect = write_step["effect"]
    orig_by_target = {r["target_row_id"]: r for r in orig["rows"]}
    entry = reads_env.get(write_step["rows_alias"], {})
    checked = 0
    for ent in write_step["rows"]:
        tgt = eval_id_expr(ent["row_id"])
        row = state.get_row(table, tgt)
        before = None if row is None else sha256(row)
        o = orig_by_target.get(tgt)
        if o is None or o["before"] != before:
            return False, {"failed": "before_image", "target": tgt, "table": table}
        if effect == "delete":
            if row is None:
                return False, {"failed": "delete_target_absent", "target": tgt,
                               "table": table}
            checked += 1
            continue
        if effect == "insert" and row is not None:
            return False, {"failed": "insert_target_exists", "target": tgt,
                           "table": table}
        src = ent["source_row_id"]
        src_row = entry.get("rows_by_id", {}).get(src)
        if src_row is None:
            return False, {"failed": "source_row_not_retained", "source": src}
        computed = {}
        for col, expr in ent.get("values", {}).items():
            computed[col] = normalize_field_value(
                eval_value(expr, {"row": src_row, "row_id": src,
                                  "reads": reads_env}, 0), 0, col)
        if sha256(computed) != o["after"]:
            return False, {"failed": "output_bytes_differ", "target": tgt,
                           "table": table}
        checked += 1
    return True, {"targets_checked": checked, "effect": effect, "table": table,
                  "before_images_match": True}


def _translate_abort(abort):
    code = abort["code"]
    if code == "guard_failed":
        return "Blocked", "blocked_guard"
    if code in ("pk_conflict", "delete_missing", "write_source_missing"):
        return "Blocked", "blocked_pk_conflict"
    return "Blocked", "blocked_step_failure"


# ---------------------------------------------------------------------------
class Ledger:
    def __init__(self, path):
        from .store import ManagedDB
        self.db = ManagedDB(path)

    def close(self):
        self.db.close()

    # ------------------------------------------------------------ state
    def head(self):
        return self.db.head()

    def state(self, generation=None):
        return self.db.state(generation)

    def snapshots(self, generation=None):
        """{table: {row_id: row}} of a generation (default: committed head)."""
        gen = self.head()["generation"] if generation is None else generation
        out = {}
        for t in self.db.tables_for_generation(gen):
            out[t] = {}
        for t, rid, row in self.db.rows_for_generation(gen):
            out[t][rid] = row
        return out

    def history(self):
        return [{"generation": g["generation"], "epoch": g["epoch"],
                 "parent": g["parent"], "kind": g["kind"],
                 "batch_ord": g["batch_ord"],
                 "revocation_id": g["revocation_id"],
                 "row_counts": g["row_counts"], "cause": g["cause"]}
                for g in self.db.generations()]

    def ledger(self):
        return self.db.batches()

    def seed(self, rows):
        """Create the initial managed state as one committed generation.

        `rows` = {table: {row_id: {column: value}}}.  Empty tables are retained
        so that a later "empty read" batch can be defined against them.  Only
        valid before any batch is ingested; seeding twice is rejected.
        """
        if self.db.ingest_batches():
            raise LedgerError("cannot seed after batches have been ingested")
        head = self.head()
        info = self.db.generation_info(head["generation"])
        if info["kind"] != "genesis" or info["row_counts"]:
            raise LedgerError("state is already seeded")
        st = self.db.state(head["generation"])
        for table, table_rows in rows.items():
            if not isinstance(table, str) or not table:
                raise LedgerError("table name must be a non-empty string")
            st.ensure_table(table)
            for rid, row in (table_rows or {}).items():
                if type(rid) is not int:
                    raise LedgerError(f"row_id for {table} must be an int, got {rid!r}")
                if not isinstance(row, dict):
                    raise LedgerError(f"row {table}/{rid} must be an object")
                for col, val in row.items():
                    if not isinstance(col, str) or not col:
                        raise LedgerError(f"column name for {table}/{rid} must be a string")
                    # Validate the v1 value domain up front.
                    from .interpreter import normalize_field_value
                    normalize_field_value(val, -1, col)
                st.put_row(table, int(rid), row)
        with self.db.tx():
            self.db._create_generation_locked(
                parent=head["generation"], kind="seed", cause="seed", state=st,
                batch_ord=None, revocation_id=None,
                trace={"events": [{"event": "seed",
                                   "tables": sorted(rows)}]})
        return self.head()

    # ----------------------------------------------------------- ingest
    def ingest(self, program, idempotency_key=None):
        """Validate and durably register a program.  Does not execute it."""
        prog = validate_program(program)
        lid = prog["logical_batch_id"]
        phash = program_digest(prog)
        raw = self.db.ingest_row(lid)
        if raw is not None:
            if raw["program_hash"] != phash or canon(raw["program"]) != canon(prog):
                raise LedgerError(
                    f"logical id {lid!r} already ingested with different program bytes")
            return {"logical_batch_id": lid, "batch_ord": raw["batch_ord"],
                    "program_hash": phash, "already_ingested": True}
        if idempotency_key is not None:
            prior = self._idem_lookup(idempotency_key)
            if prior is not None:
                if prior["program_hash"] != phash:
                    raise LedgerError(
                        f"idempotency key {idempotency_key!r} already used with a "
                        f"different program")
                return {"logical_batch_id": prior["logical_batch_id"],
                        "batch_ord": prior["batch_ord"], "program_hash": phash,
                        "already_ingested": True}
        batch_ord = self.db.append_ingest(lid, prog, phash)
        if idempotency_key is not None:
            self._idem_store(idempotency_key, lid, batch_ord, phash)
        return {"logical_batch_id": lid, "batch_ord": batch_ord,
                "program_hash": phash, "already_ingested": False}

    def _ensure_idem(self):
        self.db.conn.execute(
            "CREATE TABLE IF NOT EXISTS idempotency ("
            "key TEXT PRIMARY KEY, logical_batch_id TEXT NOT NULL, "
            "batch_ord INTEGER NOT NULL, program_hash TEXT NOT NULL, "
            "created_at TEXT NOT NULL)")

    def _idem_lookup(self, key):
        self._ensure_idem()
        r = self.db.conn.execute("SELECT * FROM idempotency WHERE key=?",
                                 (key,)).fetchone()
        if r is None:
            return None
        return {"logical_batch_id": r["logical_batch_id"],
                "batch_ord": r["batch_ord"], "program_hash": r["program_hash"]}

    def _idem_store(self, key, lid, batch_ord, phash):
        with self.db.tx():
            self._ensure_idem()
            self.db.conn.execute(
                "INSERT INTO idempotency(key,logical_batch_id,batch_ord,program_hash,"
                "created_at) VALUES(?,?,?,?,?)",
                (key, lid, batch_ord, phash, utc_now()))

    # -------------------------------------------------------------- run
    def run_ingested(self, logical_batch_id):
        """Execute an ingested batch in its immutable ingest order (idempotent)."""
        raw = self.db.ingest_row(logical_batch_id)
        if raw is None:
            raise LedgerError(f"unknown logical batch id {logical_batch_id!r}")
        existing = self.db.batch_by_logical_id(logical_batch_id)
        if existing is not None:
            return existing
        pending = [b for b in self.db.ingest_batches()
                   if b["batch_ord"] < raw["batch_ord"]
                   and self.db.batch_by_ord(b["batch_ord"]) is None]
        if pending:
            raise LedgerError(
                f"batch {raw['batch_ord']} cannot run before earlier batches "
                f"{[b['batch_ord'] for b in pending]}; time order is immutable")
        head = self.head()
        info = self.db.generation_info(head["generation"])
        if info["kind"] == "revocation":
            raise LedgerError(
                "the committed head is a revocation generation; normal ingestion is "
                "closed in that lineage (evidence remains readable)")
        prog = raw["program"]
        working = self.db.state(head["generation"])
        try:
            evidence = execute_program(prog, working)
        except BatchAbort as exc:
            evidence = {"ok": False, "abort": exc.as_dict(), "reads": [],
                        "writes": [], "guards": [], "read_contents": {},
                        "read_footprint": {}, "write_footprint": {},
                        "action_taken": False}
        phash = raw["program_hash"]
        parent = head["generation"]
        key_ranges = self._key_ranges(prog)
        if evidence["ok"]:
            trace = {"events": [{"event": "batch_commit",
                                 "batch_ord": raw["batch_ord"],
                                 "logical_batch_id": logical_batch_id}]}
            with self.db.tx():
                gen = self.db._create_generation_locked(
                    parent=parent, kind="batch", cause="batch", state=working,
                    batch_ord=raw["batch_ord"], revocation_id=None, trace=trace)
                self.db.append_ledger(raw, evidence, evidence, None, phash, parent,
                                      "success", gen["generation"],
                                      key_ranges=key_ranges)
        else:
            outcome = {"ok": False, "abort": evidence["abort"]}
            with self.db.tx():
                self.db.append_ledger(raw, evidence, outcome, evidence["abort"],
                                      phash, parent, "failed", None,
                                      key_ranges=key_ranges)
        return self.db.batch_by_logical_id(logical_batch_id)

    def submit(self, program, idempotency_key=None):
        ing = self.ingest(program, idempotency_key=idempotency_key)
        return self.run_ingested(ing["logical_batch_id"])

    # ------------------------------------------------------- partitions
    @staticmethod
    def _key_ranges(prog):
        part = prog.get("partition")
        if not part:
            return None
        return {"table": part["table"],
                "ranges": [[int(lo), int(hi)] for lo, hi in part["ranges"]]}

    @staticmethod
    def _in_ranges(rid, ranges):
        return any(lo <= rid <= hi for lo, hi in ranges)

    # ------------------------------------------------------------- plan
    def plan_revocation(self, revocation_id, target_logical_id):
        """Build a selective revocation plan without touching the official head."""
        if not isinstance(revocation_id, str) or not revocation_id:
            raise ValueError("revocation_id must be a non-empty string")
        target = self.db.batch_by_logical_id(target_logical_id)
        if target is None:
            raise LedgerError(f"unknown target batch {target_logical_id!r}")
        if target["status"] != "success":
            raise LedgerError(f"only a successful batch can be revoked; "
                              f"{target_logical_id!r} is {target['status']}")
        existing = self.db.revocation(revocation_id)
        if existing is None:
            prior = self.db.conn.execute(
                "SELECT revocation_id FROM revocation WHERE target_batch_ord=?",
                (target["batch_ord"],)).fetchone()
            if prior is not None:
                raise LedgerError(
                    f"batch {target_logical_id!r} was already revoked by "
                    f"{prior['revocation_id']!r}; a batch can be revoked once")
        if existing is not None:
            if (existing["target_logical_id"] != target_logical_id
                    or existing["target_batch_ord"] != target["batch_ord"]):
                raise LedgerError(
                    f"revocation id {revocation_id!r} is already bound to "
                    f"{existing['target_logical_id']!r} and cannot be rebound")
            if self._has_applied_plan(revocation_id):
                raise LedgerError(f"revocation {revocation_id!r} was already applied; "
                                  f"a revocation id can be applied at most once")

        head = self.head()
        current_promise = self.db.fingerprint()
        base_gen = target["parent_generation"]
        base_state = self.db.state(base_gen)
        base_promise = state_promise(base_state)
        request = {"revocation_id": revocation_id,
                   "target_logical_id": target_logical_id,
                   "target_batch_ord": target["batch_ord"]}
        request_digest = sha256(request)

        # Stage 1: bind the revocation and record the captured base (no official
        # change).  A crash from here on leaves a `planning` stage that
        # recover_interrupted_plan() can rebuild from the committed base.
        with self.db.tx():
            if self.db.revocation(revocation_id) is None:
                self.db.conn.execute(
                    "INSERT INTO revocation(revocation_id,target_batch_ord,"
                    "target_logical_id,target_generation,request_digest,request_json,"
                    "base_generation,base_epoch,state_promise,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (revocation_id, target["batch_ord"], target_logical_id,
                     target["generation"], request_digest, canon(request), base_gen,
                     self.db.generation_info(base_gen)["epoch"], base_promise,
                     utc_now()))
            plan_id = content_id("plan", {"revocation_id": revocation_id,
                                          "target": target_logical_id,
                                          "current_generation": head["generation"]})
            old = self.db.plan(plan_id)
            if old is not None and old["phase"] == "applied":
                raise LedgerError("plan already applied")
            self.db.conn.execute(
                "INSERT OR REPLACE INTO generation_plan(plan_id,revocation_id,phase,"
                "current_generation,current_epoch,current_promise,base_promise,"
                "upstream_end,first_target_ord,plan_payload_json,plan_digest,"
                "created_at,planned_generation,applied_at,applied_epoch,result_json) "
                "VALUES(?,?,'planning',?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (plan_id, revocation_id, head["generation"], head["epoch"],
                 current_promise, base_promise, target["batch_ord"],
                 target["batch_ord"], "{}", "pending", utc_now(),
                 None, None, None, None))

        # Stage 2: compute entirely in memory (pure replay + proofs).
        computed = self._compute(
            revocation_id=revocation_id, target=target, base_state=base_state,
            base_promise=base_promise, current_promise=current_promise,
            current_generation=head["generation"])
        payload = computed["payload"]
        plan_digest = sha256({"payload": payload,
                              "current_promise": current_promise,
                              "base_promise": base_promise})

        # Stage 3: persist the completed plan, still with no official change.
        with self.db.tx():
            row = self.db.conn.execute(
                "SELECT phase FROM generation_plan WHERE plan_id=?",
                (plan_id,)).fetchone()
            if row is None or row["phase"] != "planning":
                raise LedgerError("plan stage was lost or altered; retry planning")
            self.db.conn.execute(
                "UPDATE generation_plan SET phase='planned', plan_payload_json=?, "
                "plan_digest=?, planned_generation=? WHERE plan_id=?",
                (canon(payload), plan_digest, computed["planned_generation"], plan_id))
            self.db.conn.execute("DELETE FROM revocation_mapping WHERE plan_id=?",
                                 (plan_id,))
            self.db.conn.execute("DELETE FROM revocation_diff WHERE plan_id=?",
                                 (plan_id,))
            for m in computed["mapping"]:
                self.db.conn.execute(
                    "INSERT INTO revocation_mapping(plan_id,batch_ord,"
                    "logical_batch_id,disposition,reason_code,reuse_kind,detail_json) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (plan_id, m["batch_ord"], m["logical_batch_id"],
                     m["disposition"], m["reason_code"], m.get("reuse_kind"),
                     canon(m["detail"])))
            for d in computed["diff"]:
                self.db.conn.execute(
                    "INSERT INTO revocation_diff(plan_id,scope,tbl,row_id,old_digest,"
                    "new_digest,cause) VALUES(?,?,?,?,?,?,?)",
                    (plan_id, "head", d["table"], d["row_id"], d["old"], d["new"],
                     d["cause"]))
        return self.plan_view(plan_id)

    def _has_applied_plan(self, revocation_id):
        return self.db.conn.execute(
            "SELECT 1 FROM generation_plan WHERE revocation_id=? AND phase='applied'",
            (revocation_id,)).fetchone() is not None

    # -------------------------------------------------- derivation core
    def _compute(self, revocation_id, target, base_state, base_promise,
                 current_promise, current_generation):
        target_ord = target["batch_ord"]
        all_batches = self.db.batches()
        prior_success = [b for b in all_batches
                         if b["batch_ord"] < target_ord and b["status"] == "success"]
        later = [b for b in all_batches if b["batch_ord"] > target_ord]
        later_success = [b for b in later if b["status"] == "success"]
        later_failed = [b for b in later if b["status"] == "failed"]

        # The revocation's declared influence domain: the target's written
        # identities plus its complete read scope (every explicit map identity,
        # including absent and predicate-excluded ones).  This is a superset of
        # "keys whose values can change", so skipping is never justified by
        # "write sets happen not to overlap".
        target_scope = {}
        for r in target["evidence"]["reads"]:
            target_scope.setdefault(r["table"], set()).update(
                c["row_id"] for c in r["classification"])
        influence_tables = sorted(set(target["write_footprint"]) | set(target_scope))
        influence = {
            "target_batch_ord": target_ord,
            "write_keys": {t: sorted(v) for t, v in sorted(target["write_footprint"].items())},
            "read_scope_keys": {t: sorted(v) for t, v in sorted(target_scope.items())},
            "tables": influence_tables,
        }

        # Structural skip eligibility: explicit declared partition covering all
        # read-map and write identities of the batch, disjoint from the target's
        # key/scope domain on the same table.
        candidates = set()
        eligibility = {}
        part_tables = {}
        for b in later_success:
            part = b["key_ranges"]
            why = []
            ok = part is not None
            if not ok:
                why.append("no declared partition")
            if ok:
                part_tables[b["batch_ord"]] = part
                # The declared partition is meaningful only if every read-map
                # and write identity actually lives on the partitioned table.
                for rs in program_reads(b["program"]):
                    if rs["table"] != part["table"]:
                        ok = False
                        why.append(f"read table {rs['table']!r} is not the "
                                   f"partitioned table {part['table']!r}")
                        break
                    ids = [e["row_id"] for e in rs["map"]]
                    if not all(self._in_ranges(i, part["ranges"]) for i in ids):
                        ok = False
                        why.append(f"read map {rs['alias']} leaves partition")
                        break
            if ok:
                for w in b["evidence"]["writes"]:
                    if w["table"] != part["table"]:
                        ok = False
                        why.append(f"write table {w['table']!r} is not the "
                                   f"partitioned table {part['table']!r}")
                        break
                    ids = [row["target_row_id"] for row in w["rows"]]
                    if not all(self._in_ranges(i, part["ranges"]) for i in ids):
                        ok = False
                        why.append("write targets leave partition")
                        break
            if ok:
                for t, ids in influence["write_keys"].items():
                    if t == part["table"] and any(
                            self._in_ranges(i, part["ranges"]) for i in ids):
                        ok = False
                        why.append("partition intersects target write keys")
                        break
            if ok:
                for t, ids in influence["read_scope_keys"].items():
                    if t == part["table"] and any(
                            self._in_ranges(i, part["ranges"]) for i in ids):
                        ok = False
                        why.append("partition intersects target read scope")
                        break
            eligibility[b["batch_ord"]] = {"eligible": ok, "why": why,
                                           "partition": part}
            if ok:
                candidates.add(b["batch_ord"])

        # Iterate: derive with the current skip set; any skip whose re-checked
        # preconditions fail is demoted to a replay and we derive again.
        skip_set = set(candidates)
        # The structural eligibility above only looks at the *target's* keys and
        # read scope.  The actual revocation might additionally change keys via
        # a replayed (Changed) batch.  So we iteratively check the skip set
        # against the settled derivation: any skipped batch whose own scope
        # intersects a key that actually differs from the original head is
        # demoted to a real replay and the derivation is rebuilt.  Skip is
        # therefore never justified by "our write sets happen not to overlap".
        old_head_state = self.db.state(self.db.head()["generation"])
        while True:
            derived = self._derive(target, prior_success, later_success,
                                   base_state, skip_set, failed_success=later_failed)
            demoted = set(derived["failed_skip_proofs"])
            actual_changed = self._changed_keys(old_head_state, derived["state"])
            for b in later_success:
                bo = b["batch_ord"]
                if bo not in skip_set or bo in demoted:
                    continue
                touched = set()
                for rs in program_reads(b["program"]):
                    touched.update((rs["table"], e["row_id"]) for e in rs["map"])
                for w in b["evidence"]["writes"]:
                    touched.update((w["table"], row["target_row_id"])
                                   for row in w["rows"])
                overlap = touched & actual_changed
                if overlap:
                    demoted.add(bo)
                    eligibility[bo]["why"].append(
                        f"scope intersects actually changed keys "
                        f"{sorted(map(list, overlap))}")
            if not demoted:
                break
            skip_set -= demoted

        decisions = derived["decisions"]
        final_state = derived["state"]
        proof_details = derived["proofs"]

        # Record the final actual-disjointness evidence for every remaining skip.
        old_head = old_head_state
        actual_changed = self._changed_keys(old_head, final_state)
        for b in later_success:
            bo = b["batch_ord"]
            m = decisions[bo]
            if m["reuse_kind"] != "skipped_independent":
                continue
            part = part_tables[bo]
            own_read_keys = set()
            for rs in program_reads(b["program"]):
                own_read_keys.update((rs["table"], e["row_id"]) for e in rs["map"])
            own_write_keys = set()
            for w in b["evidence"]["writes"]:
                own_write_keys.update((w["table"], row["target_row_id"])
                                      for row in w["rows"])
            touched = own_read_keys | own_write_keys
            overlap = sorted([list(k) for k in touched & actual_changed])
            m["detail"]["actual_changed_keys_in_scope"] = overlap
            m["detail"]["declared_partition"] = part
            m["detail"]["proof"] = proof_details[bo]
            m["detail"]["why_unaffected"] = (
                "declared partition covers all read-map and write identities and is "
                "disjoint from the revoked batch's write keys and complete read scope; "
                "the re-checked read classifications and write before-images/output "
                "bytes are unchanged, and no actually changed key lies in the batch's "
                "scope")
            m["detail"]["actual_disjointness_proven"] = not overlap
            if overlap:
                raise IntegrityError(
                    f"internal: skipped batch {b['logical_batch_id']} overlaps "
                    f"changed keys {overlap}")

        # Diff old head -> new final state, attributed to the batch whose new
        # output row changed, otherwise to the revocation itself.
        cause = {}
        attributable = [target] + [x for x in all_batches
                                   if x["batch_ord"] > target_ord]
        for b in attributable:
            if b["batch_ord"] not in decisions and b["batch_ord"] != target_ord:
                continue
            if b["batch_ord"] == target_ord:
                label = f"batch:{b['batch_ord']}"
            else:
                m = decisions[b["batch_ord"]]
                if m["disposition"] not in ("Changed", "Superseded"):
                    continue
                label = f"batch:{b['batch_ord']}"
            for w in b["evidence"]["writes"]:
                for row in w["rows"]:
                    cause[(w["table"], row["target_row_id"])] = label
        diff = []
        for t in sorted(set(old_head.tables()) | set(final_state.tables())):
            ids = sorted(set(old_head.row_ids(t)) | set(final_state.row_ids(t)))
            for rid in ids:
                o = old_head.get_row(t, rid)
                n = final_state.get_row(t, rid)
                od = canon(o) if o is not None else None
                nd = canon(n) if n is not None else None
                if od == nd:
                    continue
                diff.append({"table": t, "row_id": rid, "old": od, "new": nd,
                             "cause": cause.get((t, rid), "revocation")})

        mapping = []
        for b in prior_success:
            mapping.append({
                "batch_ord": b["batch_ord"],
                "logical_batch_id": b["logical_batch_id"],
                "disposition": "Kept",
                "reason_code": "upstream_of_target",
                "reuse_kind": "prior_generation",
                "detail": {"note": REASON_TEXT["upstream_of_target"],
                           "generation": b["generation"]},
            })
        mapping.append({
            "batch_ord": target["batch_ord"],
            "logical_batch_id": target["logical_batch_id"],
            "disposition": "Superseded",
            "reason_code": "superseded_target",
            "reuse_kind": "removed",
            "detail": {"note": REASON_TEXT["superseded_target"],
                       "original_generation": target["generation"],
                       "removed_write_keys": target["write_footprint"],
                       "removed_read_scope": influence["read_scope_keys"]},
        })
        for b in later:
            m = decisions[b["batch_ord"]]
            m["detail"]["diff_keys"] = sorted(
                [[d["table"], d["row_id"]] for d in diff
                 if d["cause"] == f"batch:{b['batch_ord']}"])
            mapping.append(m)
        mapping.sort(key=lambda m: m["batch_ord"])

        payload = {
            "revocation_id": revocation_id,
            "target": {"batch_ord": target["batch_ord"],
                       "logical_batch_id": target["logical_batch_id"],
                       "generation": target["generation"]},
            "base_generation": target["parent_generation"],
            "base_promise": base_promise,
            "current_generation": current_generation,
            "current_promise": current_promise,
            "influence": influence,
            "skip_set": sorted(skip_set),
            "skip_eligibility": {str(k): v for k, v in sorted(eligibility.items())},
            "mapping": mapping,
            "diff": diff,
            "expected_state_hash": final_state.state_hash(),
            "expected_state_promise": state_promise(final_state),
            "expected_counts": {t: len(final_state.row_ids(t))
                                for t in final_state.tables()},
            "reason_text": REASON_TEXT,
            "proofs": {str(k): v for k, v in sorted(proof_details.items())},
        }
        return {"payload": payload, "mapping": mapping, "diff": diff,
                "planned_generation": current_generation + 1}

    def _derive(self, target, prior_success, later_success, base_state, skip_set,
                failed_success=None):
        """Replay later successes on the pre-target state with the given skip set.

        Every non-skipped successful batch is executed with its original program
        in original order.  A blocked batch performs no writes and leaves the
        state untouched.  A skipped batch is NOT executed: its original output
        rows are copied from its old generation after proving its read results
        and write preconditions match the current state.
        """
        state = base_state.copy()
        decisions = {}
        proofs = {}
        failed_skips = []
        for b in later_success:
            bo = b["batch_ord"]
            if bo in skip_set:
                ok, proof = self._skip_proof(state, b)
                proofs[bo] = proof
                if not ok:
                    failed_skips.append(bo)
                    continue
                self._retain_outputs(state, b)
                decisions[bo] = {
                    "batch_ord": bo,
                    "logical_batch_id": b["logical_batch_id"],
                    "disposition": "Kept",
                    "reason_code": "independent_partition",
                    "reuse_kind": "skipped_independent",
                    "detail": {"note": REASON_TEXT["independent_partition"],
                               "original_generation": b["generation"],
                               "basis": "declared_partition + full_read_scope + "
                                        "write_preconditions + actual_disjointness",
                               "executed": False},
                }
                continue
            before = state.copy()
            try:
                ev = execute_program(b["program"], state)
            except BatchAbort as exc:
                # A value-expression type error is a step failure: no writes survive.
                ev = {"ok": False, "abort": exc.as_dict(), "reads": [],
                      "writes": [], "guards": [], "read_contents": {},
                      "read_footprint": {}, "write_footprint": {},
                      "action_taken": False}
            if ev["ok"]:
                if (ev["read_contents"] == b["evidence"]["read_contents"]
                        and self._output_rows_of(ev, state) == self._outputs(b)):
                    disp, reason, kind = "Kept", "reused_identical", "replayed_identical"
                else:
                    disp, reason, kind = "Changed", "changed_replay", "replayed_new"
            else:
                disp, reason = _translate_abort(ev["abort"])
                kind = "not_written"
                state = before  # blocked: no writes survive
            decisions[bo] = {
                "batch_ord": bo,
                "logical_batch_id": b["logical_batch_id"],
                "disposition": disp,
                "reason_code": reason,
                "reuse_kind": kind,
                "detail": {
                    "note": REASON_TEXT[reason],
                    "executed": True,
                    "abort": ev["abort"],
                    "reads_match_original": ev["read_contents"] == b["evidence"]["read_contents"],
                    "outputs_match_original": ev["ok"] and
                        self._output_rows_of(ev, state) == self._outputs(b),
                    "original_generation": b["generation"],
                    "replay_reads": [
                        {"step_index": r["step_index"], "alias": r["alias"],
                         "table": r["table"], "selected_ids": r["selected_ids"],
                         "selected_count": r["selected_count"],
                         "candidate_count": r["candidate_count"]}
                        for r in ev["reads"]],
                    "replay_guards": ev["guards"],
                    "replay_writes": [
                        {"step_index": w["step_index"], "effect": w["effect"],
                         "table": w["table"],
                         "targets": [row["target_row_id"] for row in w["rows"]]}
                        for w in ev["writes"]],
                },
            }
        # Originally failed batches: historical only, never activated.
        for b in (failed_success or []):
            decisions[b["batch_ord"]] = {
                "batch_ord": b["batch_ord"],
                "logical_batch_id": b["logical_batch_id"],
                "disposition": "Blocked",
                "reason_code": "blocked_historical_failure",
                "reuse_kind": "historical_only",
                "detail": {"note": REASON_TEXT["blocked_historical_failure"],
                           "executed": False,
                           "original_abort": b["abort"],
                           "original_generation": None},
            }
        return {"state": state, "decisions": decisions, "proofs": proofs,
                "failed_skip_proofs": failed_skips}

    def _skip_proof(self, state, batch):
        """Prove a skipped batch's reads and write preconditions still hold.

        The proof simulates the batch's *own* writes on a scratch copy so that a
        later read step of the same batch sees the same earlier-step writes it
        saw originally.  Nothing is executed against the real state here; the
        caller retains the committed output rows only after this passes.

        Checks, in step order:
          * every read step: the complete map classification (absent/excluded/
            selected, including observed values) equals the original, so empty
            selections, predicate range membership and NULL decisions match;
          * every guard: still passes with the same value;
          * every write step: source rows retained, target before-images equal,
            and the computed output bytes equal the original after-images;
            then applies those bytes to the scratch copy.
        """
        reads = [s for s in batch["program"]["steps"] if s["op"] == "read"]
        writes = [s for s in batch["program"]["steps"] if s["op"] == "write"]
        orig_reads = batch["evidence"]["reads"]
        orig_writes = batch["evidence"]["writes"]
        orig_guards = batch["evidence"]["guards"]
        if len(reads) != len(orig_reads) or len(writes) != len(orig_writes):
            return False, {"failed": "step_count"}
        detail = {"read_checks": [], "guard_checks": [], "write_checks": [],
                  "executed": False}
        scratch = state.copy()
        reads_env = dict(_reads_env_from_evidence(batch))
        read_i = write_i = guard_i = 0
        for step in batch["program"]["steps"]:
            op = step["op"]
            if op == "read":
                ok, d = _read_preconditions(scratch, step, orig_reads[read_i])
                read_i += 1
                detail["read_checks"].append(d)
                if not ok:
                    detail["failed"] = "read_classification_changed"
                    return False, detail
            elif op == "guard":
                orig = orig_guards[guard_i]
                guard_i += 1
                entry = reads_env.get(step["alias"], {})
                if step["test"] == "row_count":
                    value = len(entry.get("selected_ids", []))
                else:
                    value = sum(entry["rows_by_id"][rid].get(step["field"]) or 0
                                for rid in entry.get("selected_ids", []))
                bounds = {k: step[k] for k in ("eq", "ge", "le") if k in step}
                ok = (("eq" not in bounds or value == bounds["eq"])
                      and ("ge" not in bounds or value >= bounds["ge"])
                      and ("le" not in bounds or value <= bounds["le"]))
                detail["guard_checks"].append({"step_index": step.get("step_index"),
                                               "value": value, "passed": ok,
                                               "original_value": orig.get("value")})
                if not ok or value != orig.get("value"):
                    detail["failed"] = "guard_value_changed"
                    return False, detail
            elif op == "write":
                orig = orig_writes[write_i]
                write_i += 1
                ok, d = _write_preconditions(scratch, step, orig, reads_env)
                detail["write_checks"].append(d)
                if not ok:
                    detail["failed"] = "write_precondition_changed"
                    return False, detail
                # Apply the computed output to the scratch copy for later reads.
                from .interpreter import eval_id_expr, eval_value, normalize_field_value
                entry = reads_env.get(step["rows_alias"], {})
                for ent in step["rows"]:
                    tgt = eval_id_expr(ent["row_id"])
                    if step["effect"] == "delete":
                        scratch.delete_row(step["table"], tgt)
                    else:
                        src = ent["source_row_id"]
                        row = {}
                        for col, expr in ent.get("values", {}).items():
                            row[col] = normalize_field_value(
                                eval_value(expr, {"row": entry["rows_by_id"][src],
                                                  "row_id": src,
                                                  "reads": reads_env}, 0), 0, col)
                        scratch.put_row(step["table"], tgt, row)
        detail["passed"] = True
        return True, detail

    def _retain_outputs(self, state, batch):
        gen = batch["generation"]
        for w in batch["evidence"]["writes"]:
            for row in w["rows"]:
                tgt = row["target_row_id"]
                if w["effect"] == "delete":
                    state.delete_row(w["table"], tgt)
                else:
                    old = self.db.get_row_at(gen, w["table"], tgt)
                    if old is not None:
                        state.put_row(w["table"], tgt, old)

    def _outputs(self, batch):
        gen = batch["generation"]
        out = {}
        for w in batch["evidence"]["writes"]:
            for row in w["rows"]:
                key = (w["table"], row["target_row_id"])
                out[key] = None if w["effect"] == "delete" else canon(
                    self.db.get_row_at(gen, w["table"], row["target_row_id"]))
        return out

    @staticmethod
    def _output_rows_of(evidence, state):
        out = {}
        for w in evidence["writes"]:
            for row in w["rows"]:
                key = (w["table"], row["target_row_id"])
                out[key] = None if w["effect"] == "delete" else canon(
                    state.get_row(w["table"], row["target_row_id"]))
        return out

    @staticmethod
    def _changed_keys(a, b):
        keys = set()
        for t in set(a.tables()) | set(b.tables()):
            for rid in set(a.row_ids(t)) | set(b.row_ids(t)):
                ra, rb = a.get_row(t, rid), b.get_row(t, rid)
                if (canon(ra) if ra is not None else None) != \
                        (canon(rb) if rb is not None else None):
                    keys.add((t, rid))
        return keys

    # ------------------------------------------------------------ apply
    def apply_plan(self, plan_id):
        """Atomically commit a planned result as a new generation.

        Refuses a stale plan: the head generation/epoch and the complete
        managed-data promise must still equal the values captured at plan time.
        Any new ingest, new ledger/batch row, new revocation or out-of-band
        SQLite edit changes the promise, so a plan built before it is rejected
        instead of overwriting the newer data.
        """
        plan = self.db.plan(plan_id)
        if plan is None:
            raise LedgerError(f"unknown plan {plan_id!r}")
        if plan["phase"] == "applied":
            # Response-loss retry (possibly from another connection): the
            # authoritative decision is the immutable recorded result.
            return self.revocation_result(plan["revocation_id"])["result"]
        if plan["phase"] != "planned":
            raise LedgerError(f"plan {plan_id!r} is in phase {plan['phase']!r}, "
                              f"not applyable")
        self._check_stale(plan, plan["current_promise"])
        if sha256({"payload": plan["payload"],
                   "current_promise": plan["current_promise"],
                   "base_promise": plan["base_promise"]}) != plan["plan_digest"]:
            raise StaleError("plan_tampered", "plan payload digest mismatch")
        self._verify_plan_persistence(plan)
        rev = self.db.revocation(plan["revocation_id"])
        target = self.db.batch_by_ord(plan["payload"]["target"]["batch_ord"])
        base_state = self.db.state(rev["base_generation"])
        if state_promise(base_state) != plan["base_promise"]:
            raise StaleError("base_changed", "pre-target base state no longer matches")
        all_batches = self.db.batches()
        prior_success = [b for b in all_batches
                         if b["batch_ord"] < target["batch_ord"]
                         and b["status"] == "success"]
        later_success = [b for b in all_batches
                         if b["batch_ord"] > target["batch_ord"]
                         and b["status"] == "success"]
        skip_set = set(plan["payload"]["skip_set"])
        failed_later = [b for b in all_batches
                        if b["batch_ord"] > target["batch_ord"]
                        and b["status"] == "failed"]
        derived = self._derive(target, prior_success, later_success, base_state,
                               skip_set, failed_success=failed_later)
        if derived["failed_skip_proofs"]:
            raise StaleError("skip_proof_failed",
                             "a batch planned as skipped can no longer be proven "
                             "unaffected", {"batches": derived["failed_skip_proofs"]})
        expected = {m["batch_ord"]: m for m in plan["payload"]["mapping"]}
        for bo, m in derived["decisions"].items():
            e = expected.get(bo)
            if e is None or e["disposition"] != m["disposition"]:
                raise StaleError(
                    "replay_diverged",
                    f"batch {m['logical_batch_id']!r} replays as {m['disposition']} "
                    f"but the plan says {e['disposition'] if e else None}; the plan "
                    f"is stale")
        final_state = derived["state"]
        if final_state.state_hash() != plan["payload"]["expected_state_hash"] \
                or state_promise(final_state) != plan["payload"]["expected_state_promise"]:
            raise StaleError("state_diverged",
                             "re-derived state does not match the planned state")
        planned_gen = plan["current_generation"] + 1
        if planned_gen != plan["planned_generation"]:
            raise StaleError("generation_mismatch", "generation numbering diverged")
        skipped_actual = sorted(bo for bo, m in derived["decisions"].items()
                                if m["reuse_kind"] == "skipped_independent")
        trace = {"events": [
            {"event": "revocation_apply", "revocation_id": rev["revocation_id"],
             "plan_id": plan_id, "target_batch_ord": target["batch_ord"],
             "executed_batch_ords": sorted(bo for bo, m in derived["decisions"].items()
                                           if m["detail"]["executed"]),
             "skipped_batch_ords": skipped_actual,
             "blocked_batch_ords": sorted(bo for bo, m in derived["decisions"].items()
                                          if m["disposition"] == "Blocked"),
             "mapping": [{"batch_ord": m["batch_ord"],
                          "disposition": m["disposition"],
                          "reason_code": m["reason_code"]}
                         for m in plan["payload"]["mapping"]],
             "diff_count": len(plan["payload"]["diff"])},
        ]}
        result = {
            "revocation_id": rev["revocation_id"],
            "plan_id": plan_id,
            "target": plan["payload"]["target"],
            "base_generation": rev["base_generation"],
            "previous_generation": plan["current_generation"],
            "new_generation": planned_gen,
            "new_epoch": plan["current_epoch"] + 1,
            "mapping": plan["payload"]["mapping"],
            "diff": plan["payload"]["diff"],
            "state_counts": plan["payload"]["expected_counts"],
            "state_hash": final_state.state_hash(),
            "state_promise": state_promise(final_state),
            "replayed_batch_ords": sorted(
                bo for bo, m in derived["decisions"].items()
                if m["detail"]["executed"] and m["disposition"] != "Blocked"),
            "skipped_independent_batch_ords": skipped_actual,
            "blocked_batch_ords": sorted(
                bo for bo, m in derived["decisions"].items()
                if m["disposition"] == "Blocked"),
            "applied_at": utc_now(),
        }
        with self.db.tx():
            fresh = self.db.plan(plan_id)
            if fresh is None:
                raise StaleError("plan_vanished", "plan row disappeared")
            if fresh["phase"] == "applied":
                return self.revocation_result(fresh["revocation_id"])["result"]
            if fresh["phase"] != "planned":
                raise StaleError("plan_phase_changed",
                                 f"plan phase became {fresh['phase']!r}")
            cur_head = self.db.head()
            if cur_head["generation"] != plan["current_generation"] or \
                    cur_head["epoch"] != plan["current_epoch"]:
                raise StaleError("head_moved",
                                 "head moved after planning (already applied?)")
            if self.db.fingerprint() != plan["current_promise"]:
                raise StaleError("fingerprint_changed",
                                 "managed data changed while applying")
            self.db._create_generation_locked(
                parent=plan["current_generation"], kind="revocation",
                cause="revocation", state=final_state, batch_ord=None,
                revocation_id=rev["revocation_id"], trace=trace)
            cur = self.db.conn.execute(
                "UPDATE generation_plan SET phase='applied', applied_at=?, "
                "applied_epoch=?, result_json=? WHERE plan_id=? AND phase='planned'",
                (result["applied_at"], result["new_epoch"], canon(result), plan_id))
            if cur.rowcount != 1:
                raise StaleError("concurrent_apply", "plan was applied concurrently")
            for seq, m in enumerate(plan["payload"]["mapping"]):
                # The plan's mapping rows are already persisted verbatim; the
                # applied disposition only exists in the immutable result and
                # lineage below.
                self.db.conn.execute(
                    "INSERT INTO lineage(rev_id,seq,generation,produced_by_id,"
                    "disposition,reason_code) VALUES(?,?,?,?,?,?)",
                    (rev["revocation_id"], seq, planned_gen, m["logical_batch_id"],
                     m["disposition"], m["reason_code"]))
        return result

    def _verify_plan_persistence(self, plan):
        """The persisted mapping/diff rows must equal the signed plan payload.

        This makes out-of-band edits of the human-facing plan bookkeeping
        detectable instead of silently becoming the official record after apply.
        """
        payload = plan["payload"]
        want = {m["batch_ord"]: m for m in payload.get("mapping", [])}
        have = {m["batch_ord"]: m for m in self.db.mapping(plan["plan_id"])}
        if set(want) != set(have):
            raise StaleError("plan_tampered", "persisted mapping set differs from "
                                              "the plan payload")
        for bo, m in want.items():
            h = have[bo]
            if (h["logical_batch_id"] != m["logical_batch_id"]
                    or h["disposition"] != m["disposition"]
                    or h["reason_code"] != m["reason_code"]
                    or h["reuse_kind"] != m.get("reuse_kind")
                    or canon(h["detail"]) != canon(m["detail"])):
                raise StaleError("plan_tampered",
                                 f"persisted mapping for batch {bo} was altered")
        want_d = {(d["table"], d["row_id"]): (d["old"], d["new"], d["cause"])
                  for d in payload.get("diff", [])}
        have_d = {(d["table"], d["row_id"]): (d["old"], d["new"], d["cause"])
                  for d in self.db.diff(plan["plan_id"])}
        if want_d != have_d:
            raise StaleError("plan_tampered", "persisted diff differs from the "
                                              "plan payload")

    def _check_stale(self, plan, expected_promise):
        head = self.head()
        if head["generation"] != plan["current_generation"] or \
                head["epoch"] != plan["current_epoch"]:
            raise StaleError(
                "head_moved", "committed generation/epoch moved after planning",
                {"planned_generation": plan["current_generation"],
                 "current_generation": head["generation"],
                 "planned_epoch": plan["current_epoch"],
                 "current_epoch": head["epoch"]})
        current = self.db.fingerprint()
        if current != expected_promise:
            raise StaleError(
                "fingerprint_changed",
                "managed data or history changed after planning (new batch, new "
                "revocation, or out-of-band tampering); refusing the stale plan",
                {"planned_promise": expected_promise, "current_promise": current})

    # --------------------------------------------------------- recovery
    def plan_view(self, plan_id):
        plan = self.db.plan(plan_id)
        if plan is None:
            raise LedgerError(f"unknown plan {plan_id!r}")
        p = plan["payload"]
        return {
            "plan_id": plan_id,
            "revocation_id": plan["revocation_id"],
            "phase": plan["phase"],
            "target": p.get("target"),
            "base_generation": p.get("base_generation"),
            "current_generation": plan["current_generation"],
            "current_epoch": plan["current_epoch"],
            "planned_generation": plan["planned_generation"],
            "plan_digest": plan["plan_digest"],
            "current_promise": plan["current_promise"],
            "base_promise": plan["base_promise"],
            "influence": p.get("influence"),
            "skip_set": p.get("skip_set"),
            "skip_eligibility": p.get("skip_eligibility"),
            "proofs": p.get("proofs"),
            "mapping": p.get("mapping", []),
            "diff": p.get("diff", []),
            "state_counts": p.get("expected_counts"),
            "reason_text": p.get("reason_text"),
            "applied_at": plan["applied_at"],
            "applied_epoch": plan["applied_epoch"],
            "result": plan["result"],
        }

    def revocation_result(self, revocation_id_or_plan_id):
        """Authoritative, idempotent lookup of a revocation decision.

        Used when the apply response was lost: the caller can poll by
        revocation id and get the same committed decision, never a second apply.
        """
        rev = self.db.revocation(revocation_id_or_plan_id)
        plan = None
        if rev is not None:
            plan = self.db.plan_for_revocation(rev["revocation_id"])
        else:
            plan = self.db.plan(revocation_id_or_plan_id)
            if plan is not None:
                rev = self.db.revocation(plan["revocation_id"])
        if rev is None:
            raise LedgerError(f"unknown revocation/plan {revocation_id_or_plan_id!r}")
        if plan is None:
            return {"revocation_id": rev["revocation_id"], "phase": "none",
                    "target": rev["target_logical_id"]}
        out = {"revocation_id": rev["revocation_id"], "plan_id": plan["plan_id"],
               "phase": plan["phase"], "target": rev["target_logical_id"],
               "planned_generation": plan["planned_generation"],
               "mapping": self.db.mapping(plan["plan_id"]),
               "diff": self.db.diff(plan["plan_id"])}
        if plan["phase"] == "applied":
            out["result"] = plan["result"]
            out["new_generation"] = plan["planned_generation"]
            out["applied_epoch"] = plan["applied_epoch"]
        return out

    def recover_interrupted_plan(self, revocation_id, target_logical_id, crash_before_bind=False):
        """Resume planning from an explicit persisted stage.

        `crash_before_bind` models a crash after the caller chose a revocation
        id but before any row was committed: there is nothing to replay.

        no revocation row -> fresh plan (or "nothing persisted" with the flag)
        phase 'planning'  -> delete the partial plan, replan from the committed
                             pre-target generation (which must still match its
                             recorded promise)
        phase 'planned'   -> return the completed plan
        phase 'applied'   -> return the recorded result
        """
        rev = self.db.revocation(revocation_id)
        if rev is None:
            if crash_before_bind:
                return {"action": "nothing_persisted"}
            return {"action": "started_fresh",
                    "plan": self.plan_revocation(revocation_id, target_logical_id)}
        if rev["target_logical_id"] != target_logical_id:
            raise LedgerError("revocation id is bound to a different target")
        plan = self.db.plan_for_revocation(revocation_id)
        if plan is None or plan["phase"] in ("planning",):
            base = self.db.state(rev["base_generation"])
            if state_promise(base) != rev["state_promise"]:
                raise IntegrityError("pre-target base generation was tampered with")
            if plan is not None:
                with self.db.tx():
                    self.db.conn.execute(
                        "DELETE FROM generation_plan WHERE plan_id=?", (plan["plan_id"],))
            return {"action": "replanned_from_stage",
                    "plan": self.plan_revocation(revocation_id, target_logical_id)}
        if plan["phase"] == "applied":
            return {"action": "already_applied",
                    "result": self.revocation_result(revocation_id)}
        return {"action": "plan_complete", "plan": self.plan_view(plan["plan_id"])}

    def table_at(self, generation, table):
        return {rid: row for t, rid, row in self.db.rows_for_generation(generation)
                if t == table}

    def managed_state_view(self, generation=None):
        return self.snapshots(generation)
