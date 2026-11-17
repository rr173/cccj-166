"""Closed, versioned batch-program protocol (v1).

A program is inert data.  There is no SQL, no code, no expression language
beyond constants, fields of rows already read by this batch, and exact integer
+/-.  No current time, randomness or external I/O exists anywhere in the
execution path.

Program shape
-------------
{
  "logical_batch_id": "<caller supplied stable id, unique>",
  "schema": "managed-batch/v1",
  "steps": [ <step>, ... ]
}

step :=
  {"op": "read",
   "select": {"table": "<t>", "where": <pred>},
   "alias": "<name>",
   "map": [ {"row_id": <int64>, "table": "<t>"}, ... ]}
| {"op": "write",
   "effect": "insert"|"replace"|"delete",
   "table": "<t>", "rows_alias": "<read alias>",
   "rows": [ {"source_row_id": <int64>, "row_id": <id-expr>,
              "values": {<col>: <value-expr>, ...}} , ... ]}
| {"op": "guard", "test": "row_count"|"int_sum", "alias": "<read alias>",
   "field": "<int field, int_sum only>",
   "eq"|"ge"|"le": <int>}        # every present bound must hold

pred :=
   {"kind": "all"}
 | {"kind": "and"|"or", "args": [<pred>, ...]}
 | {"kind": "not", "arg": <pred>}
 | {"kind": "eq", "field": <field-ref>, "value": <scalar>}
 | {"kind": "range", "field": <field-ref>, "lo": <int>, "hi": <int>}
 | {"kind": "is_null", "field": <field-ref>, "value": true|false}

field-ref := "<col>" | "<earlier alias>.<col>"
id-expr   := {"const": <int64>} | {"add": [{"const": <int64>}, <int>]}
value-expr:= {"const": <scalar>}
           | {"field": <field-ref>}
           | {"add"|"sub": [<value-expr>, <value-expr>]}    # integer only, exact

Identity and read semantics
---------------------------
* Every managed row has an explicit, stable int64 row_id; identity is
  (table, row_id).  Row identity is never inferred from values.
* A read result is the set of rows whose identity is listed in the step's
  explicit map AND whose current values satisfy the predicate at that moment.
  Map entries for absent rows simply do not appear in the result.
* Write target identity comes only from an explicit source_row_id mapping plus
  the row_id expression.  Insert/replace write exactly the fields given in
  "values" (missing fields become NULL, so replace is a full-row replace).
  Delete removes exactly (table, target row_id).

Predicates use strict two-valued logic over NULL:
  eq:      (v IS NOT NULL) AND (v == const)
  range:   (v IS NOT NULL) AND (lo <= v <= hi)     -- on int values
  is_null: (v IS NULL) == bool
  NOT:     plain boolean negation of that two-valued result, so NOT(eq(age,1))
           holds for rows whose age is NULL or any non-1 value.

Batch semantics
---------------
Steps execute in order; each read sees the state immediately before it,
including earlier writes of the same batch.  A failed guard or a primary-key
conflict is a failpoint: the step is not applied, failure is recorded, no later
step runs.  Successful batches commit all steps atomically.  Batch time order
is the insertion order and is immutable.

Declared relation rules
-----------------------
A ledger may install a ``managed-constraints/v1`` configuration once, before
the seed or first ingest (see ``constraints.py``).  Explicit steps then run
exactly as above, and only the completed candidate state is constrained:
deletion roots are row identities present before the batch and absent after
its steps, a CASCADE closure is applied, then surviving references and unique
keys are checked.  A violation rejects the whole batch.
"""

import copy

from .canon import content_id
from .errors import LimitError, ProtocolError

SCHEMA = "managed-batch/v1"

MAX_SELECT_DEPTH = 8
MAX_NODES = 128
MAX_STEPS = 64
MAX_ROWS_PER_STEP = 10_000
MAX_VALUE_LEN = 4096
MAX_VALUE_DEPTH = 8
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

_PROG_REQUIRED = {"logical_batch_id", "schema", "steps"}
_PROG_KEYS = _PROG_REQUIRED | {"partition"}
_WRITE_KEYS = {"op", "effect", "table", "rows_alias", "rows"}
_VALUE_OPS = ("add", "sub")


def _check(cond, msg):
    if not cond:
        raise ProtocolError(msg)


def _is_int(v):
    return type(v) is int


def _is_row_id(v):
    return _is_int(v) and INT64_MIN <= v <= INT64_MAX


def _check_field_ref(ref, aliases, step_index, context="select"):
    _check(isinstance(ref, str) and ref,
           f"step {step_index}: {context} field ref must be a non-empty string")
    if "." in ref:
        alias, _, col = ref.partition(".")
        _check(alias in aliases,
               f"step {step_index}: {context} references unknown/forward alias {alias!r}")
        _check(bool(col), f"step {step_index}: {context} field ref {ref!r} has empty column")
    else:
        _check(ref != "*", f"step {step_index}: {context} field ref may not be '*'")


def _check_constant(v, step_index, field):
    if v is None or isinstance(v, bool):
        return
    if isinstance(v, str):
        _check(len(v) <= MAX_VALUE_LEN, f"step {step_index}: string constant too long for {field}")
        return
    _check(_is_int(v), f"step {step_index}: value for {field} must be int/str/bool/null, "
                       f"got {type(v).__name__}")
    _check(INT64_MIN <= v <= INT64_MAX, f"step {step_index}: int constant out of int64 range")


def _check_value_expr(expr, aliases, step_index, depth=0, counter=None):
    counter[0] += 1
    if counter[0] > MAX_NODES:
        raise LimitError(f"step {step_index}: value expression exceeds {MAX_NODES} nodes")
    _check(depth <= MAX_VALUE_DEPTH, f"step {step_index}: value expression too deep")
    _check(isinstance(expr, dict), f"step {step_index}: value expression must be an object")
    keys = set(expr)
    if keys == {"const"}:
        _check_constant(expr["const"], step_index, "const")
    elif keys == {"field"}:
        _check_field_ref(expr["field"], aliases, step_index, context="value")
    elif len(keys) == 1 and next(iter(keys)) in _VALUE_OPS:
        args = expr[next(iter(keys))]
        _check(isinstance(args, list) and len(args) == 2,
               f"step {step_index}: {next(iter(keys))} takes [a, b]")
        for a in args:
            _check_value_expr(a, aliases, step_index, depth + 1, counter)
    else:
        raise ProtocolError(f"step {step_index}: unknown value expression {sorted(keys)}")


def _check_pred(node, aliases, step_index, depth=0, counter=None):
    counter[0] += 1
    if counter[0] > MAX_NODES:
        raise LimitError(f"step {step_index}: predicate exceeds {MAX_NODES} nodes")
    _check(depth <= MAX_SELECT_DEPTH,
           f"step {step_index}: predicate nesting exceeds {MAX_SELECT_DEPTH}")
    _check(isinstance(node, dict), f"step {step_index}: predicate node must be an object")
    kind = node.get("kind")
    if kind == "all":
        _check(set(node) == {"kind"}, f"step {step_index}: 'all' takes no extra keys")
    elif kind in ("and", "or"):
        _check(set(node) == {"kind", "args"}, f"step {step_index}: '{kind}' needs only args")
        args = node["args"]
        _check(isinstance(args, list) and 1 <= len(args) <= 32,
               f"step {step_index}: '{kind}' args must be a list of 1..32 predicates")
        for sub in args:
            _check_pred(sub, aliases, step_index, depth + 1, counter)
    elif kind == "not":
        _check(set(node) == {"kind", "arg"}, f"step {step_index}: 'not' needs exactly arg")
        _check_pred(node["arg"], aliases, step_index, depth + 1, counter)
    elif kind == "eq":
        _check(set(node) == {"kind", "field", "value"}, f"step {step_index}: eq shape")
        _check_field_ref(node["field"], aliases, step_index)
        _check_constant(node["value"], step_index, "eq")
    elif kind == "range":
        _check(set(node) == {"kind", "field", "lo", "hi"}, f"step {step_index}: range shape")
        _check_field_ref(node["field"], aliases, step_index)
        _check(_is_int(node["lo"]) and _is_int(node["hi"]),
               f"step {step_index}: range bounds must be ints")
        _check(node["lo"] <= node["hi"], f"step {step_index}: range lo > hi")
    elif kind == "is_null":
        _check(set(node) == {"kind", "field", "value"}, f"step {step_index}: is_null shape")
        _check_field_ref(node["field"], aliases, step_index)
        _check(type(node["value"]) is bool, f"step {step_index}: is_null value must be boolean")
    else:
        raise ProtocolError(f"step {step_index}: unknown predicate kind {kind!r}")


def _check_id_expr(expr, step_index, where):
    _check(isinstance(expr, dict), f"step {step_index}: {where} row_id expression must be an object")
    keys = set(expr)
    _check(keys in ({"const"}, {"add"}),
           f"step {step_index}: {where} row_id expression must be {{const}} or {{add}}")
    if "const" in expr:
        _check(_is_row_id(expr["const"]), f"step {step_index}: {where} const row_id must be int64")
        return
    add = expr["add"]
    _check(isinstance(add, list) and len(add) == 2,
           f"step {step_index}: {where} add needs [{{const}}, int]")
    inner, delta = add
    _check(_is_int(delta), f"step {step_index}: {where} add delta must be int")
    _check(isinstance(inner, dict) and set(inner) == {"const"} and _is_row_id(inner["const"]),
           f"step {step_index}: {where} add base must be {{const: int64}}")
    _check(_is_row_id(inner["const"] + delta),
           f"step {step_index}: {where} add result out of int64 range")


def _check_partition(part):
    if part is None:
        return
    _check(isinstance(part, dict) and set(part) == {"table", "ranges"},
           "partition must be {table, ranges}")
    _check(isinstance(part["table"], str) and part["table"],
           "partition.table must be a non-empty string")
    ranges = part["ranges"]
    _check(isinstance(ranges, list) and ranges, "partition.ranges must be a non-empty list")
    prev_hi = None
    clean = []
    for rng in ranges:
        _check(isinstance(rng, list) and len(rng) == 2, "partition ranges must be [lo,hi]")
        lo, hi = rng
        _check(_is_row_id(lo) and _is_row_id(hi) and lo <= hi,
               "partition range must be int64 lo<=hi")
        _check(prev_hi is None or lo > prev_hi,
               "partition ranges must be sorted and disjoint")
        prev_hi = hi
        clean.append([lo, hi])
    part["ranges"] = clean


def validate_program(program, batch_index=0):
    """Validate a program against the protocol; return a deep copy.

    Raises ProtocolError/LimitError.  Does not touch any database.
    """
    _check(isinstance(program, dict), "program must be an object")
    extra = set(program) - _PROG_KEYS
    _check(not extra, f"program top-level keys must be logical_batch_id/schema/steps/"
                      f"partition (extra {sorted(extra)})")
    _check(_PROG_REQUIRED <= set(program), "program needs logical_batch_id/schema/steps")
    _check(isinstance(program["logical_batch_id"], str)
           and 1 <= len(program["logical_batch_id"]) <= 200,
           "logical_batch_id must be a 1..200 char string")
    _check(program["schema"] == SCHEMA, f"unsupported schema {program['schema']!r}")
    steps = program["steps"]
    _check(isinstance(steps, list) and 1 <= len(steps) <= MAX_STEPS,
           f"steps must be a list of 1..{MAX_STEPS}")
    prog = copy.deepcopy(program)
    _check_partition(prog.get("partition"))
    aliases = {}          # alias -> {"table": ..., "map": set((table,row_id))}
    seen_aliases = set()
    steps_out = []
    for i, st in enumerate(prog["steps"]):
        _check(isinstance(st, dict), f"step {i}: must be an object")
        op = st.get("op")
        if op == "read":
            _check(set(st) == {"op", "select", "alias", "map"}, f"step {i}: read shape")
            alias = st["alias"]
            _check(isinstance(alias, str) and alias and "." not in alias,
                   f"step {i}: alias must be a non-empty string without '.'")
            _check(alias not in seen_aliases, f"step {i}: duplicate alias {alias!r}")
            sel = st["select"]
            _check(isinstance(sel, dict) and set(sel) == {"table", "where"},
                   f"step {i}: select shape")
            _check(isinstance(sel["table"], str) and sel["table"],
                   f"step {i}: table must be non-empty")
            _check_pred(sel["where"], aliases, i, counter=[0])
            m = st["map"]
            _check(isinstance(m, list) and len(m) <= MAX_ROWS_PER_STEP,
                   f"step {i}: map must be a list <= {MAX_ROWS_PER_STEP}")
            seen_ids = set()
            clean_map = []
            for ent in m:
                _check(isinstance(ent, dict) and set(ent) == {"row_id", "table"},
                       f"step {i}: map entry shape")
                _check(_is_row_id(ent["row_id"]), f"step {i}: map row_id must be int64")
                _check(isinstance(ent["table"], str) and ent["table"], f"step {i}: map table")
                _check(ent["table"] == sel["table"],
                       f"step {i}: map table must equal select table")
                key = ("__read__", ent["row_id"])
                _check(key not in seen_ids, f"step {i}: duplicate map identity {key}")
                seen_ids.add(key)
                clean_map.append({"row_id": ent["row_id"], "table": ent["table"]})
            seen_aliases.add(alias)
            aliases[alias] = {"table": sel["table"], "map": seen_ids}
            steps_out.append({"op": "read", "select": sel, "alias": alias, "map": clean_map})
        elif op == "write":
            _check(set(st) == _WRITE_KEYS, f"step {i}: write shape")
            _check(st["effect"] in ("insert", "replace", "delete"),
                   f"step {i}: bad effect")
            _check(isinstance(st["table"], str) and st["table"],
                   f"step {i}: table must be non-empty")
            alias = st["rows_alias"]
            _check(alias in aliases, f"step {i}: write consumes unknown/forward alias {alias!r}")
            # Write table may differ from the read table: the target identity is
            # explicit and value expressions only read fields of rows this batch
            # has read, so cross-table materialisation is still a closed read->
            # write mapping.
            rows = st["rows"]
            _check(isinstance(rows, list), f"step {i}: rows must be a list")
            if st["effect"] == "delete":
                _check(len(rows) <= MAX_ROWS_PER_STEP, f"step {i}: too many delete mappings")
            else:
                _check(1 <= len(rows) <= MAX_ROWS_PER_STEP,
                       f"step {i}: insert/replace needs 1..{MAX_ROWS_PER_STEP} rows")
            seen_src = set()
            clean_rows = []
            for ent in rows:
                _check(isinstance(ent, dict),
                       f"step {i}: write row must be an object")
                keys = set(ent)
                _check(keys in ({"source_row_id", "row_id"},
                                {"source_row_id", "row_id", "values"}),
                       f"step {i}: write row keys")
                if st["effect"] == "delete":
                    _check("values" not in keys, f"step {i}: delete writes no values")
                _check(_is_row_id(ent["source_row_id"]),
                       f"step {i}: source_row_id must be int64")
                src = int(ent["source_row_id"])
                _check(src not in seen_src, f"step {i}: duplicate source_row_id {src}")
                seen_src.add(src)
                _check(("__read__", src) in aliases[alias]["map"],
                       f"step {i}: source_row_id {src} is not in alias {alias!r}'s explicit map")
                _check_id_expr(ent["row_id"], i, f"write target of source {src}")
                values = ent.get("values", {})
                _check(isinstance(values, dict), f"step {i}: values must be an object")
                _check(len(values) <= 64, f"step {i}: too many columns")
                for col, expr in values.items():
                    _check(isinstance(col, str) and col and "." not in col,
                           f"step {i}: bad column name {col!r}")
                    _check_value_expr(expr, aliases, i, counter=[0])
                out = {"source_row_id": src, "row_id": ent["row_id"]}
                if "values" in ent:
                    out["values"] = values
                clean_rows.append(out)
            steps_out.append({"op": "write", "effect": st["effect"], "table": st["table"],
                              "rows_alias": alias, "rows": clean_rows})
        elif op == "guard":
            keys = set(st)
            _check(keys <= {"op", "test", "alias", "field", "eq", "ge", "le"}
                   and {"op", "test", "alias"} <= keys,
                   f"step {i}: guard shape")
            test = st["test"]
            _check(test in ("row_count", "int_sum"), f"step {i}: guard test")
            alias = st["alias"]
            _check(alias in aliases, f"step {i}: guard consumes unknown/forward alias {alias!r}")
            if test == "row_count":
                _check("field" not in st, f"step {i}: row_count guard takes no field")
            else:
                _check(isinstance(st.get("field"), str) and st["field"],
                       f"step {i}: int_sum needs field")
                _check("." not in st["field"],
                       f"step {i}: guard field must be on the read alias's table")
            ops = [k for k in ("eq", "ge", "le") if k in st]
            _check(ops, f"step {i}: guard needs at least one of eq/ge/le")
            for k in ops:
                _check(_is_int(st[k]), f"step {i}: guard {k} must be int")
            steps_out.append(copy.deepcopy(st))
            continue
        else:
            raise ProtocolError(f"step {i}: unknown op {op!r}")
    prog["steps"] = steps_out
    return prog


def program_digest(program):
    return content_id("prog", program)


def program_reads(program):
    """Static read scope derived from the program (public definition)."""
    out = []
    for i, st in enumerate(program["steps"]):
        if st["op"] == "read":
            out.append({
                "step_index": i,
                "alias": st["alias"],
                "table": st["select"]["table"],
                "map": st["map"],
                "where": st["select"]["where"],
            })
    return out


def program_tables(program):
    return sorted({st["table"] for st in program["steps"] if st["op"] in ("read", "write")})
