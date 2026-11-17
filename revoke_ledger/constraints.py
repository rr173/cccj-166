"""Declarative relational integrity for managed generations.

A constraint configuration is inert JSON:

    {
      "schema": "managed-constraints/v1",
      "rules": [
        {"name": "acct_u", "kind": "unique",
         "table": "account", "fields": ["tenant", "code"]},
        {"name": "acct_owner_fk", "kind": "foreign_key",
         "child": "account", "field": "owner_id",
         "parent": "owner", "on_delete": "RESTRICT"}
      ]
    }

Every rule has a stable, unique ``name``.  A rule is installed once, before
the seed (or before the first ingest), and can never be replaced.

Semantics
---------
* Foreign key values are int64 row ids of the parent table, or null.  ``bool``
  is not an integer here.  A missing field means null.  Null never requires a
  parent row.
* A unique key ignores every tuple that contains a null.  Remaining tuples
  compare by *type and value*: ``1``, ``true`` and ``"1"`` are three different
  keys.
* Self references, multi-level references and reference cycles are allowed.

Batch-final checking
--------------------
The interpreter's explicit steps run normally and see their own transient
writes.  Only after every explicit step and guard succeeded is the final
candidate state checked.  A batch may therefore insert a child before its
parent, or swap two rows' unique values, as long as the *completed* state is
consistent.

Delete semantics are defined by row identity, not field values:

1. Compare the pre-batch state with the candidate state after all explicit
   steps.  Every row that existed before and is absent now is a deletion root.
   A row_id deleted and re-created in the same batch did not disappear.
2. Starting from those roots, repeatedly delete every surviving candidate row
   that references a deleted row through a CASCADE rule, until no new row is
   deleted.  Each row is deleted at most once; cycles terminate.
3. Only after that closure is complete are surviving RESTRICT references, other
   foreign keys and unique keys checked.  A RESTRICT child removed by another
   cascade path therefore does not block; an inserted child pointing at a
   parent that never existed is an ordinary violation, never a silent cascade.

The closure is a pure function of (pre state, candidate state, rule set); no
SQLite foreign keys or triggers are involved.  Results are deterministic: BFS
starts from sorted roots, rules are processed by name and children by row_id.
The first path that reaches a row is its recorded trigger chain; additional
deleted parents that also reference it are recorded as ``extra_parents``.
"""

import copy
from collections import deque

from .canon import canon, sha256
from .errors import ProtocolError
from .interpreter import value_type

CONSTRAINT_SCHEMA = "managed-constraints/v1"
ON_DELETE_POLICIES = ("CASCADE", "RESTRICT")
MAX_RULES = 1000
MAX_NAME_LEN = 200
MAX_FIELDS = 64


def _check(cond, message):
    if not cond:
        raise ProtocolError(message)


def _is_name(v):
    return isinstance(v, str) and 1 <= len(v) <= MAX_NAME_LEN


def _is_table(v):
    return isinstance(v, str) and 1 <= len(v) <= MAX_NAME_LEN


def _is_column(v):
    return (isinstance(v, str) and 1 <= len(v) <= MAX_NAME_LEN
            and "." not in v)


def validate_config(config):
    """Validate a constraint configuration and return a normalized copy.

    Raises ProtocolError.  Nothing is persisted by this function, so an
    invalid configuration can never be installed half-way.
    """
    _check(isinstance(config, dict), "constraint config must be an object")
    _check(set(config) == {"schema", "rules"},
           "constraint config must have exactly 'schema' and 'rules'")
    _check(config["schema"] == CONSTRAINT_SCHEMA,
           f"unsupported constraint schema {config['schema']!r}")
    rules = config["rules"]
    _check(isinstance(rules, list) and len(rules) <= MAX_RULES,
           f"constraint rules must be a list of <= {MAX_RULES}")
    out = []
    names = set()
    for i, rule in enumerate(rules):
        _check(isinstance(rule, dict), f"rule {i}: must be an object")
        name = rule.get("name")
        _check(_is_name(name), f"rule {i}: name must be a 1..{MAX_NAME_LEN} char string")
        _check(name not in names, f"rule {i}: duplicate rule name {name!r}")
        names.add(name)
        kind = rule.get("kind")
        _check(kind in ("unique", "foreign_key"),
               f"rule {name!r}: kind must be 'unique' or 'foreign_key'")
        if kind == "unique":
            _check(set(rule) == {"name", "kind", "table", "fields"},
                   f"rule {name!r}: unique rule takes name/kind/table/fields")
            _check(_is_table(rule["table"]), f"rule {name!r}: table must be a string")
            fields = rule["fields"]
            _check(isinstance(fields, list) and 1 <= len(fields) <= MAX_FIELDS,
                   f"rule {name!r}: fields must be a non-empty list")
            seen = set()
            clean = []
            for f in fields:
                _check(_is_column(f),
                       f"rule {name!r}: field must be a non-empty column name "
                       f"without '.'")
                _check(f not in seen, f"rule {name!r}: duplicate field {f!r}")
                seen.add(f)
                clean.append(f)
            out.append({"name": name, "kind": "unique", "table": rule["table"],
                        "fields": clean})
        else:
            _check(set(rule) == {"name", "kind", "child", "field", "parent",
                                 "on_delete"},
                   f"rule {name!r}: foreign_key rule takes "
                   f"name/kind/child/field/parent/on_delete")
            _check(_is_table(rule["child"]), f"rule {name!r}: child must be a table")
            _check(_is_table(rule["parent"]), f"rule {name!r}: parent must be a table")
            _check(_is_column(rule["field"]),
                   f"rule {name!r}: field must be a non-empty column name")
            _check(rule["on_delete"] in ON_DELETE_POLICIES,
                   f"rule {name!r}: on_delete must be one of "
                   f"{list(ON_DELETE_POLICIES)}")
            out.append({"name": name, "kind": "foreign_key",
                        "child": rule["child"], "field": rule["field"],
                        "parent": rule["parent"],
                        "on_delete": rule["on_delete"]})
    return {"schema": CONSTRAINT_SCHEMA, "rules": out}


def _tagged(value):
    """Type-preserving JSON representation used in reasons and evidence."""
    return {"type": value_type(value), "value": value}


def _unique_token(row, fields):
    """(type, value) tuple for a unique key, or None if any part is null."""
    token = []
    for field in fields:
        v = row.get(field)
        if v is None:
            return None
        token.append((value_type(v), v))
    return tuple(token)


def _fk_value(row, field):
    """Return (ok, value).  Missing/null -> (True, None); bool -> (False, ...)."""
    v = row.get(field)
    if v is None:
        return True, None
    if type(v) is int:
        return True, v
    return False, v


def _reason_sort_key(reason):
    ids = reason.get("row_ids")
    primary = reason.get("row_id")
    if primary is None and ids:
        primary = ids[0]
    return (reason["rule"], reason["code"], reason["table"],
            -1 if primary is None else primary, canon(reason))


class ConstraintSet:
    """An immutable, validated rule set plus the integrity algorithms."""

    def __init__(self, config):
        self.config = validate_config(config)
        self.config_json = canon(self.config)
        self.config_hash = sha256(self.config)
        self._rules = self.config["rules"]
        self._fk = sorted((r for r in self._rules if r["kind"] == "foreign_key"),
                          key=lambda r: r["name"])
        self._unique = sorted((r for r in self._rules if r["kind"] == "unique"),
                              key=lambda r: r["name"])

    # -------------------------------------------------------------- views
    def view(self):
        return {"schema": CONSTRAINT_SCHEMA, "rules": copy.deepcopy(self._rules),
                "config_hash": self.config_hash}

    def rule_names(self):
        return sorted(r["name"] for r in self._rules)

    def participating_tables(self):
        tables = set()
        for r in self._rules:
            if r["kind"] == "unique":
                tables.add(r["table"])
            else:
                tables.add(r["child"])
                tables.add(r["parent"])
        return tables

    # --------------------------------------------------------- validation
    def _check_fk_rows(self, state, deleted_keys, reasons):
        for rule in self._fk:
            child, field, parent = rule["child"], rule["field"], rule["parent"]
            for rid in state.row_ids(child):
                row = state.get_row(child, rid)
                ok, value = _fk_value(row, field)
                base = {"rule": rule["name"], "table": child, "row_id": rid,
                        "field": field, "parent": parent,
                        "referenced_row_id": value}
                if not ok:
                    reasons.append(dict(base, code="fk_type",
                                        value=_tagged(value)))
                    continue
                if value is None:
                    continue
                if state.get_row(parent, value) is None:
                    if (parent, value) in deleted_keys \
                            and rule["on_delete"] == "RESTRICT":
                        reasons.append(dict(
                            base, code="restrict_violation",
                            deleted_parent=[parent, value],
                            detail="surviving child references a parent row "
                                   "deleted by this batch under RESTRICT"))
                    else:
                        reasons.append(dict(base, code="fk_missing"))

    def _check_unique_rows(self, state, reasons):
        for rule in self._unique:
            groups = {}
            table, fields = rule["table"], rule["fields"]
            for rid in state.row_ids(table):
                row = state.get_row(table, rid)
                token = _unique_token(row, fields)
                if token is None:
                    continue          # any null -> not a participant
                groups.setdefault(token, []).append(rid)
            for token, ids in groups.items():
                if len(ids) < 2:
                    continue
                ids = sorted(ids)
                reasons.append({
                    "code": "unique_violation",
                    "rule": rule["name"],
                    "table": table,
                    "row_ids": ids,
                    "row_id": ids[0],
                    "fields": list(fields),
                    "values": [_tagged(val) for val in token_values(token)],
                })

    def validate_state(self, state, deleted_keys=()):
        """Check a completed state without deleting anything.

        Used for the seed, for skip proofs and as the last stage of
        :meth:`finalize`.  Returns a deterministically sorted reason list.
        """
        reasons = []
        self._check_fk_rows(state, set(deleted_keys), reasons)
        self._check_unique_rows(state, reasons)
        # Two distinct rows may each report the same missing parent; the
        # reason payloads stay distinct because they carry the child identity,
        # but an exact duplicate is dropped deterministically.
        unique = {}
        for r in reasons:
            unique.setdefault(canon(r), r)
        reasons = list(unique.values())
        reasons.sort(key=_reason_sort_key)
        return reasons

    # ------------------------------------------------------------ closure
    def finalize(self, pre_state, candidate_state, writes=()):
        """Apply cascade deletes and validate a batch's completed state.

        ``pre_state`` is the state at batch start; ``candidate_state`` is the
        state after all explicit steps and is mutated by cascade deletes.
        ``writes`` is the interpreter's write-step evidence, used only to mark
        which deletions were explicit.
        """
        explicit_deleted = set()
        explicit_touched = set()
        for step in writes or ():
            table = step["table"]
            for row in step["rows"]:
                key = (table, row["target_row_id"])
                explicit_touched.add(key)
                if step["effect"] == "delete":
                    explicit_deleted.add(key)

        # Step 1: deletion roots by row identity.
        tables = set(pre_state.tables()) | set(candidate_state.tables())
        roots = []
        for table in sorted(tables):
            for rid in sorted(pre_state.row_ids(table)):
                if candidate_state.get_row(table, rid) is None:
                    roots.append((table, rid))

        deleted = {}
        queue = deque()
        for key in sorted(roots):
            table, rid = key
            deleted[key] = {
                "table": table, "row_id": rid,
                "explicit": key in explicit_deleted,
                "rule": None,
                "trigger_chain": [],
                "root": [table, rid],
                "inserted_in_batch": False,
                "extra_parents": set(),
            }
            queue.append(key)

        # Step 2: cascade closure (BFS, sorted, each row at most once).
        while queue:
            pt, pr = queue.popleft()
            for rule in self._fk:
                if rule["parent"] != pt or rule["on_delete"] != "CASCADE":
                    continue
                child, field = rule["child"], rule["field"]
                for crid in sorted(set(pre_state.row_ids(child)) |
                                   set(candidate_state.row_ids(child))):
                    child_row = candidate_state.get_row(child, crid)
                    if child_row is None:
                        child_row = pre_state.get_row(child, crid)
                    if child_row is None:
                        continue
                    ok, value = _fk_value(child_row, field)
                    if not ok or value is None or value != pr:
                        continue
                    ckey = (child, crid)
                    if ckey in deleted:
                        if ckey != (pt, pr):
                            deleted[ckey]["extra_parents"].add((pt, pr))
                        continue
                    parent_info = deleted[(pt, pr)]
                    hop = {"from_table": pt, "from_row_id": pr,
                           "to_table": child, "to_row_id": crid,
                           "rule": rule["name"], "on_delete": "CASCADE"}
                    deleted[ckey] = {
                        "table": child, "row_id": crid,
                        "explicit": False,
                        "rule": rule["name"],
                        "trigger_chain": parent_info["trigger_chain"] + [hop],
                        "root": list(parent_info["root"]),
                        "inserted_in_batch": pre_state.get_row(child, crid) is None,
                        "extra_parents": set(),
                    }
                    candidate_state.delete_row(child, crid)
                    queue.append(ckey)

        deleted_keys = set(deleted)
        reasons = self.validate_state(candidate_state, deleted_keys)

        records = []
        for key in sorted(deleted):
            info = deleted[key]
            records.append({
                "table": info["table"],
                "row_id": info["row_id"],
                "explicit": info["explicit"],
                "implicit": not info["explicit"],
                "rule": info["rule"],
                "trigger_chain": info["trigger_chain"],
                "root": info["root"],
                "inserted_in_batch": info["inserted_in_batch"],
                "extra_parents": [list(p) for p in sorted(info["extra_parents"])],
            })
        implicit = [[r["table"], r["row_id"]] for r in records if r["implicit"]]
        return {
            "ok": not reasons,
            "reasons": reasons,
            "roots": [{"table": t, "row_id": r,
                       "explicit": (t, r) in explicit_deleted}
                      for (t, r) in sorted(roots)],
            "deleted": records,
            "explicit_deletes": sorted([list(k) for k in explicit_deleted]),
            "explicit_touched": sorted([list(k) for k in explicit_touched]),
            "implicit_deletes": implicit,
        }

    # -------------------------------------------------- revocation scopes
    def dependency_scope(self, state, keys):
        """Keys whose value can affect this key set through a declared rule.

        Used by the selective-revocation skip proof.  Starting from the
        batch's explicit read/write identities (and its recorded cascade
        deletes), it expands across FK edges in both directions, cascade
        reachability and unique-key groups.  A skipped batch may ignore a
        changed key outside this closure; inside it the batch must be replayed.
        """
        scope = set(keys)
        frontier = list(scope)
        while frontier:
            table, rid = frontier.pop()
            # Rows that reference this row through any FK rule.
            for rule in self._fk:
                if rule["parent"] != table:
                    continue
                child, field = rule["child"], rule["field"]
                for crid in state.row_ids(child):
                    ok, value = _fk_value(state.get_row(child, crid), field)
                    if ok and value is not None and value == rid:
                        key = (child, crid)
                        if key not in scope:
                            scope.add(key)
                            frontier.append(key)
            row = state.get_row(table, rid)
            if row is None:
                continue
            # The parent this row references.
            for rule in self._fk:
                if rule["child"] != table:
                    continue
                ok, value = _fk_value(row, rule["field"])
                if ok and value is not None:
                    key = (rule["parent"], value)
                    if key not in scope:
                        scope.add(key)
                        frontier.append(key)
            # Rows sharing (or potentially sharing) a unique key group.
            for rule in self._unique:
                if rule["table"] != table:
                    continue
                token = _unique_token(row, rule["fields"])
                if token is None:
                    continue
                for orid in state.row_ids(table):
                    orow = state.get_row(table, orid)
                    if orow is not None and \
                            _unique_token(orow, rule["fields"]) == token:
                        key = (table, orid)
                        if key not in scope:
                            scope.add(key)
                            frontier.append(key)
        return scope


def token_values(token):
    """Values out of a (type, value) token, only for reason rendering."""
    return [v for (_t, v) in token]
