"""Versioned, closed protocols for the offline CDC transaction ingestor.

Two inert JSON protocols live here, both versioned and both validated before
anything is persisted:

1. ``managed-cdc-source/v1``  - immutable ingestion configuration for one
   upstream source: its starting transaction sequence, the source-table ->
   managed-table mapping, each source table's ordered composite primary key,
   and which columns reference which source tables.

2. ``managed-cdc-fragment/v1`` - one wire fragment of a source transaction.
   A transaction is assembled from exactly one ``begin`` fragment, one ``end``
   fragment and the complete set of ``change`` fragments indexed 1..N.
   Delivery order is unconstrained; the ``end`` fragment may arrive first.

A third program schema, ``managed-cdc/v1``, is the *deterministic program*
built from one complete, ready source transaction when it is advanced.  It is
a normal ledger batch program (validated here, executed by ``cdcexec.py``), so
CDC commits participate in the existing generation, constraint and selective
revocation machinery without a special side channel.

Nothing in this module performs I/O: validation is a pure function, so an
invalid configuration or fragment can never be installed half-way.
"""

from .errors import ProtocolError

CDC_CONFIG_SCHEMA = "managed-cdc-source/v1"
CDC_FRAGMENT_SCHEMA = "managed-cdc-fragment/v1"
CDC_PROGRAM_SCHEMA = "managed-cdc/v1"

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

MAX_SOURCE_TABLES = 256
MAX_KEY_FIELDS = 32
MAX_REF_COLUMNS = 64
MAX_NAME_LEN = 200
MAX_VALUE_LEN = 4096
MAX_CHANGES = 100_000
MAX_FRAGMENT_BYTES = 16 * 1024 * 1024

OUTCOMES = ("commit", "abort")
CHANGE_OPS = ("insert", "update", "delete")
FRAGMENT_PARTS = ("begin", "change", "end")

_ENVELOPE_KEYS = {"schema", "source_id", "tx_seq", "tx_id", "part"}
_BEGIN_KEYS = _ENVELOPE_KEYS
_CHANGE_KEYS = _ENVELOPE_KEYS | {"index", "table", "op", "before", "after"}
_END_KEYS = _ENVELOPE_KEYS | {"change_count", "outcome"}
_CONFIG_KEYS = {"schema", "source_id", "start_seq", "tables"}
_TABLE_CFG_KEYS = {"source_table", "managed_table", "key_fields", "references"}
_REF_KEYS = {"column", "references"}
_PROGRAM_KEYS = {"logical_batch_id", "schema", "source_id", "tx_seq", "tx_id",
                 "allocations", "changes"}
_PROGRAM_CHANGE_KEYS = {"index", "op", "source_table", "managed_table",
                        "target_row_id", "key_json", "before", "after"}
_ALLOCATION_KEYS = {"source_table", "managed_table", "key_json", "row_id"}


def _check(cond, message):
    if not cond:
        raise ProtocolError(message)


def _is_name(v):
    return isinstance(v, str) and 1 <= len(v) <= MAX_NAME_LEN


def _is_int64(v):
    return type(v) is int and INT64_MIN <= v <= INT64_MAX


def _is_nonneg_int64(v):
    return type(v) is int and 0 <= v <= INT64_MAX


def valid_scalar(v):
    """The source value domain: non-null int64 / text / bool, or null."""
    if v is None or type(v) is bool:
        return True
    if type(v) is int:
        return INT64_MIN <= v <= INT64_MAX
    if type(v) is str:
        return len(v) <= MAX_VALUE_LEN
    return False


def _check_row(row, where, require_all_fields=()):
    """Validate a full before/after row object.

    `require_all_fields` columns must be present (key comparison distinguishes
    a missing field from an explicit null, so presence -- not just non-nullity
    -- is checked here).  Reference columns are encoded as key arrays and are
    accepted structurally here; their declared width and component types are
    enforced against the installed configuration separately.
    """
    _check(isinstance(row, dict), f"{where}: row must be an object")
    for col, val in row.items():
        _check(isinstance(col, str) and 1 <= len(col) <= MAX_NAME_LEN,
               f"{where}: column names must be non-empty strings")
        if isinstance(val, list):
            for j, part in enumerate(val):
                _check(part is not None and valid_scalar(part),
                       f"{where}: array component {j} must be a non-null "
                       f"int64/text/bool")
            continue
        _check(valid_scalar(val),
               f"{where}: column {col!r} must hold int64/text/bool/null, "
               f"got {type(val).__name__}")
    for f in require_all_fields:
        _check(f in row, f"{where}: required key field {f!r} is missing")
        _check(row[f] is not None, f"{where}: key field {f!r} must be non-null")
        _check(valid_scalar(row[f]), f"{where}: key field {f!r} has an invalid value")


# ---------------------------------------------------------------------------
# source configuration
# ---------------------------------------------------------------------------
def validate_source_config(config):
    """Validate and normalize a ``managed-cdc-source/v1`` configuration.

    Raises ProtocolError on any problem.  Pure: the caller persists only the
    returned normalized copy, so an invalid config can never half-install.
    """
    _check(isinstance(config, dict), "CDC source config must be an object")
    _check(set(config) == _CONFIG_KEYS,
           f"CDC source config must have exactly {sorted(_CONFIG_KEYS)}")
    _check(config["schema"] == CDC_CONFIG_SCHEMA,
           f"unsupported CDC source schema {config['schema']!r}")
    _check(_is_name(config["source_id"]),
           "source_id must be a non-empty string")
    _check(_is_nonneg_int64(config["start_seq"]),
           "start_seq must be a non-negative int64")
    tables = config["tables"]
    _check(isinstance(tables, list) and 1 <= len(tables) <= MAX_SOURCE_TABLES,
           f"tables must be a non-empty list of <= {MAX_SOURCE_TABLES}")

    out_tables = []
    source_names, managed_names = set(), set()
    for i, tc in enumerate(tables):
        where = f"table config {i}"
        _check(isinstance(tc, dict), f"{where}: must be an object")
        _check(set(tc) == _TABLE_CFG_KEYS,
               f"{where}: keys must be {sorted(_TABLE_CFG_KEYS)}")
        src, mng = tc["source_table"], tc["managed_table"]
        _check(_is_name(src), f"{where}: source_table must be a non-empty string")
        _check(_is_name(mng), f"{where}: managed_table must be a non-empty string")
        _check(src not in source_names,
               f"{where}: duplicate source_table {src!r}")
        _check(mng not in managed_names,
               f"{where}: duplicate managed_table {mng!r}")
        source_names.add(src)
        managed_names.add(mng)

        key_fields = tc["key_fields"]
        _check(isinstance(key_fields, list)
               and 1 <= len(key_fields) <= MAX_KEY_FIELDS,
               f"{where}: key_fields must be a non-empty list")
        clean_keys = []
        for f in key_fields:
            _check(isinstance(f, str) and 1 <= len(f) <= MAX_NAME_LEN and "." not in f,
                   f"{where}: key field must be a non-empty string without '.'")
            _check(f not in clean_keys,
                   f"{where}: duplicate key field {f!r}")
            clean_keys.append(f)

        refs_in = tc.get("references", [])
        _check(isinstance(refs_in, list) and len(refs_in) <= MAX_REF_COLUMNS,
               f"{where}: references must be a list")
        clean_refs = []
        ref_cols = set()
        for rc in refs_in:
            _check(isinstance(rc, dict), f"{where}: each reference must be an object")
            _check(set(rc) == _REF_KEYS,
                   f"{where}: reference keys must be {sorted(_REF_KEYS)}")
            col, target = rc["column"], rc["references"]
            _check(isinstance(col, str) and 1 <= len(col) <= MAX_NAME_LEN,
                   f"{where}: reference column must be a non-empty string")
            _check(_is_name(target),
                   f"{where}: reference target must be a non-empty string")
            _check(col not in clean_keys,
                   f"{where}: reference column {col!r} cannot also be a key field")
            _check(col not in ref_cols,
                   f"{where}: duplicate reference column {col!r}")
            ref_cols.add(col)
            clean_refs.append({"column": col, "references": target})
        out_tables.append({"source_table": src, "managed_table": mng,
                           "key_fields": clean_keys, "references": clean_refs})

    # Reference targets must name a table declared by this same config.
    for tc in out_tables:
        for rc in tc["references"]:
            _check(rc["references"] in source_names,
                   f"table {tc['source_table']!r}: reference column "
                   f"{rc['column']!r} points at undeclared source table "
                   f"{rc['references']!r}")
    return {"schema": CDC_CONFIG_SCHEMA, "source_id": config["source_id"],
            "start_seq": config["start_seq"], "tables": out_tables}


# ---------------------------------------------------------------------------
# fragments
# ---------------------------------------------------------------------------
def validate_fragment(frag):
    """Validate one ``managed-cdc-fragment/v1`` fragment (config-independent).

    Full row value-domain checks are included.  Config-dependent checks
    (known table, declared key fields, declared reference columns) are the
    ingestor's job because it owns the installed configuration.
    """
    _check(isinstance(frag, dict), "fragment must be an object")
    schema = frag.get("schema")
    _check(schema == CDC_FRAGMENT_SCHEMA,
           f"unsupported CDC fragment schema {schema!r}")
    for k in ("source_id", "tx_id"):
        _check(_is_name(frag.get(k)), f"fragment {k!r} must be a non-empty string")
    _check(_is_nonneg_int64(frag.get("tx_seq")),
           "fragment tx_seq must be a non-negative int64")
    part = frag.get("part")
    _check(part in FRAGMENT_PARTS,
           f"fragment part must be one of {list(FRAGMENT_PARTS)}")

    if part == "begin":
        _check(set(frag) == _BEGIN_KEYS,
               f"begin fragment keys must be {sorted(_BEGIN_KEYS)}")
    elif part == "end":
        _check(set(frag) == _END_KEYS,
               f"end fragment keys must be {sorted(_END_KEYS)}")
        _check(_is_nonneg_int64(frag.get("change_count")),
               "end fragment change_count must be a non-negative int64")
        _check(frag["change_count"] <= MAX_CHANGES,
               f"change_count exceeds limit {MAX_CHANGES}")
        _check(frag.get("outcome") in OUTCOMES,
               f"end outcome must be one of {list(OUTCOMES)}")
    else:
        _check(set(frag) == _CHANGE_KEYS,
               f"change fragment keys must be {sorted(_CHANGE_KEYS)}")
        _check(type(frag.get("index")) is int and frag["index"] >= 1
               and frag["index"] <= MAX_CHANGES,
               "change index must be an int >= 1")
        _check(frag.get("op") in CHANGE_OPS,
               f"change op must be one of {list(CHANGE_OPS)}")
        _check(isinstance(frag.get("table"), str) and frag["table"],
               "change table must be a non-empty string")
        before, after = frag.get("before"), frag.get("after")
        op = frag["op"]
        if op == "insert":
            _check(before is None, "insert change requires before=null")
            _check(isinstance(after, dict), "insert change requires an after row")
            _check_row(after, f"change {frag['index']} after")
        elif op == "delete":
            _check(isinstance(before, dict), "delete change requires a before row")
            _check(after is None, "delete change requires after=null")
            _check_row(before, f"change {frag['index']} before")
        else:
            _check(isinstance(before, dict) and isinstance(after, dict),
                   "update change requires both before and after rows")
            _check_row(before, f"change {frag['index']} before")
            _check_row(after, f"change {frag['index']} after")
    # Return a shallow-normalized copy (input is already plain JSON data).
    return dict(frag)


def check_fragment_against_config(frag, cfg):
    """Config-dependent fragment validation.  Raises ProtocolError.

    Checks that change rows name a declared table, carry every ordered key
    field as a non-null domain value, and that declared reference columns are
    either null or an array of non-null key components whose width matches the
    referenced table's composite key.
    """
    if frag["part"] != "change":
        return
    by_name = {tc["source_table"]: tc for tc in cfg["tables"]}
    tc = by_name.get(frag["table"])
    _check(tc is not None,
           f"change {frag['index']}: source table {frag['table']!r} is not "
           f"declared by source {cfg['source_id']!r}")
    # An update must keep the same primary key; a key change is expressed as a
    # delete of the old key plus an insert of the new key.
    if frag["op"] == "update":
        before_key = [frag["before"][f] for f in tc["key_fields"]]
        after_key = [frag["after"][f] for f in tc["key_fields"]]
        _check(before_key == after_key,
               f"change {frag['index']}: an update cannot change the primary "
               f"key {before_key!r} -> {after_key!r}; express it as a delete "
               f"followed by an insert")
    ref_by_col = {r["column"]: r for r in tc["references"]}
    for label, row in (("before", frag["before"]), ("after", frag["after"])):
        if row is None:
            continue
        where = f"change {frag['index']} {label}"
        for f in tc["key_fields"]:
            _check(f in row, f"{where}: key field {f!r} is missing")
            _check(row[f] is not None, f"{where}: key field {f!r} is null")
            _check(valid_scalar(row[f]),
                   f"{where}: key field {f!r} holds an invalid value")
        for col, ref in ref_by_col.items():
            if col not in row or row[col] is None:
                continue
            v = row[col]
            target_cfg = by_name[ref["references"]]
            _check(isinstance(v, list),
                   f"{where}: reference column {col!r} must be null or a key "
                   f"array (an object/number is not a key)")
            _check(len(v) == len(target_cfg["key_fields"]),
                   f"{where}: reference column {col!r} must have exactly "
                   f"{len(target_cfg['key_fields'])} component(s) for table "
                   f"{ref['references']!r}, got {len(v)}")
            for j, part in enumerate(v):
                _check(part is not None and valid_scalar(part),
                       f"{where}: reference column {col!r} component {j} must "
                       f"be a non-null int64/text/bool")
        # Unknown extra columns are allowed (open row schema), but their values
        # still have to be in the closed value domain.
        for col, val in row.items():
            if col in ref_by_col:
                continue
            _check(valid_scalar(val),
                   f"{where}: column {col!r} must hold int64/text/bool/null")


# ---------------------------------------------------------------------------
# deterministic CDC batch program (built at advance time)
# ---------------------------------------------------------------------------
def validate_cdc_program(prog):
    """Validate a ``managed-cdc/v1`` deterministic transaction program."""
    _check(isinstance(prog, dict), "CDC program must be an object")
    _check(set(prog) == _PROGRAM_KEYS,
           f"CDC program keys must be {sorted(_PROGRAM_KEYS)}")
    _check(prog["schema"] == CDC_PROGRAM_SCHEMA,
           f"unsupported CDC program schema {prog['schema']!r}")
    _check(_is_name(prog["logical_batch_id"]),
           "CDC program logical_batch_id must be a non-empty string")
    _check(_is_name(prog["source_id"]), "CDC program source_id must be a string")
    _check(_is_nonneg_int64(prog["tx_seq"]), "CDC program tx_seq must be int64")
    _check(_is_name(prog["tx_id"]), "CDC program tx_id must be a string")

    allocations = prog["allocations"]
    _check(isinstance(allocations, list), "allocations must be a list")
    seen_keys, seen_ids = set(), set()
    for a in allocations:
        _check(isinstance(a, dict) and set(a) == _ALLOCATION_KEYS,
               f"allocation keys must be {sorted(_ALLOCATION_KEYS)}")
        _check(_is_name(a["source_table"]) and _is_name(a["managed_table"]),
               "allocation table names must be non-empty strings")
        _check(isinstance(a["key_json"], str) and a["key_json"],
               "allocation key_json must be a non-empty string")
        _check(_is_int64(a["row_id"]), "allocation row_id must be int64")
        k = (a["source_table"], a["key_json"])
        _check(k not in seen_keys, "duplicate identity allocation")
        seen_keys.add(k)
        i = (a["managed_table"], a["row_id"])
        _check(i not in seen_ids, "two identities mapped to one row_id")
        seen_ids.add(i)

    changes = prog["changes"]
    _check(isinstance(changes, list) and len(changes) <= MAX_CHANGES,
           f"changes must be a list of <= {MAX_CHANGES}")
    indices = []
    for ch in changes:
        _check(isinstance(ch, dict) and set(ch) == _PROGRAM_CHANGE_KEYS,
               f"CDC change keys must be {sorted(_PROGRAM_CHANGE_KEYS)}")
        _check(type(ch["index"]) is int and ch["index"] >= 1,
               "CDC change index must be an int >= 1")
        _check(ch["op"] in CHANGE_OPS, "CDC change op is invalid")
        _check(_is_name(ch["source_table"]) and _is_name(ch["managed_table"]),
               "CDC change table names must be non-empty strings")
        _check(_is_int64(ch["target_row_id"]), "CDC target_row_id must be int64")
        _check(isinstance(ch["key_json"], str) and ch["key_json"],
               "CDC change key_json must be a non-empty string")
        for label in ("before", "after"):
            v = ch[label]
            if v is not None:
                _check(isinstance(v, dict), f"CDC change {label} must be null/object")
                for col, val in v.items():
                    _check(isinstance(col, str) and col,
                           "CDC row column names must be non-empty strings")
                    _check(valid_scalar(val),
                           f"CDC {label} column {col!r} has an invalid value")
        indices.append(ch["index"])
    _check(indices == sorted(indices) and len(set(indices)) == len(indices),
           "CDC change indices must be unique and ordered")
    if changes:
        _check(indices == list(range(1, len(indices) + 1)),
               "CDC changes must cover the contiguous range 1..change_count")
    return dict(prog)
