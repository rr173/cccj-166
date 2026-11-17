"""Closed, versioned relational batch-program protocol (managed-relational/v1).

Like ``managed-batch/v1`` a program is inert data: no SQL, no code, no
expression language beyond constants, fields of relations this batch formed,
and exact integer +/-.  No current time, randomness or external I/O exists
anywhere in the execution path.

Program shape
-------------
{
  "logical_batch_id": "<caller supplied stable id, unique>",
  "schema": "managed-relational/v1",
  "steps": [ <step>, ... ],
  "partition": {"table": "<t>", "ranges": [[lo,hi]]}     // optional
}

step :=
  {"op": "read", "select": {"table": "<t>", "where": <pred>},
   "alias": "<name>", "map": [{"row_id": <int64>, "table": "<t>"}, ...],
   "row_id_field": "<name>"}                 // optional: expose source row_id
| {"op": "join", "left": "<alias>", "right": "<alias>",
   "how": "inner"|"left",
   "on": [{"left": "<field-of-left>", "right": "<field-of-right>"}, ...],
   "alias": "<name>"}
| {"op": "project", "from": "<alias>", "alias": "<name>",
   "select": [{"from": "<field-ref>", "as": "<name>"}, ...]}
| {"op": "group", "from": "<alias>", "alias": "<name>",
   "keys": [<field-ref>, ...],
   "aggregates": [{"op": "count_star", "as": "<name>"} |
                  {"op": "count"|"sum"|"min"|"max",
                   "field": "<field-ref>", "as": "<name>"}, ...]}
| {"op": "write", "effect": "insert"|"replace"|"delete", "table": "<t>",
   "from": "<alias>", "target": <int-expr>,
   "values": {"<col>": <value-expr>, ...}}   // values forbidden for delete
| {"op": "guard", "test": "row_count"|"int_sum", "relation": "<alias>",
   "field": "<field-ref, int_sum only>", "eq"|"ge"|"le": <int>}

field-ref := a field of the referenced relation.  Reads of managed tables are
*open* relations (any simple column name is defined; a missing column reads as
null).  project/group outputs are *closed* relations (exactly the declared
names).  join outputs are qualified: "left.<f>" / "right.<f>" (nested joins
nest the qualifier).  Name conflicts and references to undefined fields are
rejected at validation time with the step, the ref and the defined names.

int-expr / value-expr := {"const": <scalar>} | {"field": <field-ref>}
                         | {"add"|"sub": [<expr>, <expr>]}   (int only, exact)
A write target must evaluate to a non-null int64 per derived row; it is never
taken from a traversal index or a hash.

Relation semantics (execution is in relational.py; this module validates):
* Each row occurrence has a stable identity (see relational.py) that does not
  depend on the order of the caller's read maps.
* Join keys compare by type AND value: 1, true and "1" are different keys.
  Any null or missing key component never matches (on either side).
* Joins preserve duplicate row contributions: two left rows and three right
  rows sharing a key produce six inner results.  A left join keeps every
  unmatched left row once, with null right-side fields.
* Group keys are typed (1/true/"1" differ) but null and a missing field are
  the same empty key component.  Empty input yields no groups when keys are
  present, and exactly one all-null/0 row for a keyless global aggregation.
"""

import copy

from .errors import LimitError, ProtocolError
from .protocol import (INT64_MAX, INT64_MIN, MAX_NODES, MAX_ROWS_PER_STEP,
                       MAX_STEPS, MAX_VALUE_DEPTH, _check, _check_constant,
                       _check_partition, _check_pred, _is_int, _is_row_id)

RELATIONAL_SCHEMA = "managed-relational/v1"

MAX_ON_KEYS = 8
MAX_GROUP_KEYS = 8
MAX_AGGREGATES = 16
MAX_SELECT_ITEMS = 64
MAX_VALUE_COLS = 64

_PROG_REQUIRED = {"logical_batch_id", "schema", "steps"}
_PROG_KEYS = _PROG_REQUIRED | {"partition"}
_AGG_OPS = ("count_star", "count", "sum", "min", "max")
_VALUE_OPS = ("add", "sub")


# ---------------------------------------------------------------------------
# relation schemas (static field-reference checking)
# ---------------------------------------------------------------------------
def _open():
    return {"kind": "open"}


def _closed(fields):
    return {"kind": "closed", "fields": tuple(fields)}


def _join_schema(left, right):
    return {"kind": "join", "left": left, "right": right}


def _check_simple_name(name, step_index, what):
    _check(isinstance(name, str) and name and "." not in name,
           f"step {step_index}: {what} must be a non-empty name without '.', "
           f"got {name!r}")


def _check_rel_field(ref, schema, step_index, context):
    """Validate a field reference against a relation schema (static)."""
    _check(isinstance(ref, str) and ref,
           f"step {step_index}: {context} field ref must be a non-empty string")
    kind = schema["kind"]
    if kind == "open":
        _check("." not in ref,
               f"step {step_index}: {context} field ref {ref!r} must be a "
               f"simple column name on a table-read relation")
        return
    if kind == "closed":
        _check(ref in schema["fields"],
               f"step {step_index}: {context} references undefined field "
               f"{ref!r}; defined fields: {sorted(schema['fields'])}")
        return
    for side in ("left", "right"):
        prefix = side + "."
        if ref.startswith(prefix):
            _check_rel_field(ref[len(prefix):], schema[side], step_index,
                             context)
            return
    raise ProtocolError(
        f"step {step_index}: {context} field ref {ref!r} must be qualified "
        f"with 'left.' or 'right.' on a join relation")


def _check_rel_value_expr(expr, schema, step_index, context, depth=0,
                          counter=None, target=False):
    counter[0] += 1
    if counter[0] > MAX_NODES:
        raise LimitError(f"step {step_index}: {context} expression exceeds "
                         f"{MAX_NODES} nodes")
    _check(depth <= MAX_VALUE_DEPTH,
           f"step {step_index}: {context} expression too deep")
    _check(isinstance(expr, dict),
           f"step {step_index}: {context} expression must be an object")
    keys = set(expr)
    if keys == {"const"}:
        if target:
            _check(_is_int(expr["const"])
                   and INT64_MIN <= expr["const"] <= INT64_MAX,
                   f"step {step_index}: {context} const must be an int64 "
                   f"integer (not bool/text/null)")
        else:
            _check_constant(expr["const"], step_index, context)
    elif keys == {"field"}:
        _check_rel_field(expr["field"], schema, step_index, context)
    elif len(keys) == 1 and next(iter(keys)) in _VALUE_OPS:
        op = next(iter(keys))
        args = expr[op]
        _check(isinstance(args, list) and len(args) == 2,
               f"step {step_index}: {context} {op} takes [a, b]")
        for a in args:
            _check_rel_value_expr(a, schema, step_index, context, depth + 1,
                                  counter, target=target)
    else:
        raise ProtocolError(
            f"step {step_index}: {context} unknown expression {sorted(keys)}")


# ---------------------------------------------------------------------------
# step validators
# ---------------------------------------------------------------------------
def _validate_read(st, i, relations, read_aliases):
    _check({"op", "select", "alias", "map"} <= set(st)
           <= {"op", "select", "alias", "map", "row_id_field"},
           f"step {i}: read shape")
    alias = st["alias"]
    _check_simple_name(alias, i, "alias")
    _check(alias not in relations, f"step {i}: duplicate alias {alias!r}")
    sel = st["select"]
    _check(isinstance(sel, dict) and set(sel) == {"table", "where"},
           f"step {i}: select shape")
    _check(isinstance(sel["table"], str) and sel["table"],
           f"step {i}: table must be non-empty")
    _check_pred(sel["where"], read_aliases, i, counter=[0])
    rid_field = st.get("row_id_field")
    if rid_field is not None:
        _check_simple_name(rid_field, i, "row_id_field")
    m = st["map"]
    _check(isinstance(m, list) and len(m) <= MAX_ROWS_PER_STEP,
           f"step {i}: map must be a list <= {MAX_ROWS_PER_STEP}")
    seen_ids = set()
    clean_map = []
    for ent in m:
        _check(isinstance(ent, dict) and set(ent) == {"row_id", "table"},
               f"step {i}: map entry shape")
        _check(_is_row_id(ent["row_id"]), f"step {i}: map row_id must be int64")
        _check(isinstance(ent["table"], str) and ent["table"]
               and ent["table"] == sel["table"],
               f"step {i}: map table must equal select table")
        _check(ent["row_id"] not in seen_ids,
               f"step {i}: duplicate map identity {ent['row_id']}")
        seen_ids.add(ent["row_id"])
        clean_map.append({"row_id": ent["row_id"], "table": ent["table"]})
    relations[alias] = _open()
    read_aliases[alias] = sel["table"]
    out = {"op": "read", "select": sel, "alias": alias, "map": clean_map}
    if rid_field is not None:
        out["row_id_field"] = rid_field
    return out


def _validate_join(st, i, relations):
    _check(set(st) == {"op", "left", "right", "how", "on", "alias"},
           f"step {i}: join shape")
    left, right = st["left"], st["right"]
    _check(left in relations,
           f"step {i}: join references unknown/forward relation {left!r}")
    _check(right in relations,
           f"step {i}: join references unknown/forward relation {right!r}")
    _check(st["how"] in ("inner", "left"),
           f"step {i}: join how must be 'inner' or 'left'")
    on = st["on"]
    _check(isinstance(on, list) and 1 <= len(on) <= MAX_ON_KEYS,
           f"step {i}: join on must be a list of 1..{MAX_ON_KEYS} key pairs")
    clean_on = []
    for kp in on:
        _check(isinstance(kp, dict) and set(kp) == {"left", "right"},
               f"step {i}: join key pair must be {{left, right}}")
        _check_rel_field(kp["left"], relations[left], i, "join key (left)")
        _check_rel_field(kp["right"], relations[right], i, "join key (right)")
        clean_on.append({"left": kp["left"], "right": kp["right"]})
    alias = st["alias"]
    _check_simple_name(alias, i, "alias")
    _check(alias not in relations, f"step {i}: duplicate alias {alias!r}")
    relations[alias] = _join_schema(relations[left], relations[right])
    return {"op": "join", "left": left, "right": right, "how": st["how"],
            "on": clean_on, "alias": alias}


def _validate_project(st, i, relations):
    _check(set(st) == {"op", "from", "select", "alias"},
           f"step {i}: project shape")
    src = st["from"]
    _check(src in relations,
           f"step {i}: project references unknown/forward relation {src!r}")
    select = st["select"]
    _check(isinstance(select, list) and 1 <= len(select) <= MAX_SELECT_ITEMS,
           f"step {i}: project select must be a list of "
           f"1..{MAX_SELECT_ITEMS} items")
    seen = set()
    clean = []
    for item in select:
        _check(isinstance(item, dict) and set(item) == {"from", "as"},
               f"step {i}: project item must be {{from, as}}")
        _check_rel_field(item["from"], relations[src], i, "project")
        _check_simple_name(item["as"], i, "projection name")
        _check(item["as"] not in seen,
               f"step {i}: projection name conflict: {item['as']!r} is "
               f"produced twice")
        seen.add(item["as"])
        clean.append({"from": item["from"], "as": item["as"]})
    alias = st["alias"]
    _check_simple_name(alias, i, "alias")
    _check(alias not in relations, f"step {i}: duplicate alias {alias!r}")
    relations[alias] = _closed([c["as"] for c in clean])
    return {"op": "project", "from": src, "select": clean, "alias": alias}


def _validate_group(st, i, relations):
    _check(set(st) == {"op", "from", "keys", "aggregates", "alias"},
           f"step {i}: group shape")
    src = st["from"]
    _check(src in relations,
           f"step {i}: group references unknown/forward relation {src!r}")
    keys = st["keys"]
    _check(isinstance(keys, list) and len(keys) <= MAX_GROUP_KEYS,
           f"step {i}: group keys must be a list of 0..{MAX_GROUP_KEYS} "
           f"field refs")
    _check(len(set(keys)) == len(keys),
           f"step {i}: duplicate group key")
    for k in keys:
        _check_rel_field(k, relations[src], i, "group key")
    aggs = st["aggregates"]
    _check(isinstance(aggs, list) and len(aggs) <= MAX_AGGREGATES,
           f"step {i}: aggregates must be a list of 0..{MAX_AGGREGATES}")
    names = set(keys)           # key output names are the key refs themselves
    clean_aggs = []
    for a in aggs:
        _check(isinstance(a, dict), f"step {i}: aggregate must be an object")
        opk = a.get("op")
        _check(opk in _AGG_OPS, f"step {i}: unknown aggregate {opk!r}")
        if opk == "count_star":
            _check(set(a) == {"op", "as"},
                   f"step {i}: count_star takes op/as only")
        else:
            _check(set(a) == {"op", "field", "as"},
                   f"step {i}: {opk} takes op/field/as")
            _check_rel_field(a["field"], relations[src], i,
                             f"aggregate {opk}")
        _check_simple_name(a["as"], i, "aggregate name")
        _check(a["as"] not in names,
               f"step {i}: name conflict: {a['as']!r} is both a group key "
               f"and an aggregate output")
        names.add(a["as"])
        clean_aggs.append(copy.deepcopy(a))
    alias = st["alias"]
    _check_simple_name(alias, i, "alias")
    _check(alias not in relations, f"step {i}: duplicate alias {alias!r}")
    relations[alias] = _closed(list(keys) + [a["as"] for a in clean_aggs])
    return {"op": "group", "from": src, "keys": list(keys),
            "aggregates": clean_aggs, "alias": alias}


def _validate_write(st, i, relations):
    _check({"op", "effect", "table", "from", "target"} <= set(st)
           <= {"op", "effect", "table", "from", "target", "values"},
           f"step {i}: write shape")
    _check(st["effect"] in ("insert", "replace", "delete"),
           f"step {i}: bad effect")
    _check(isinstance(st["table"], str) and st["table"],
           f"step {i}: table must be non-empty")
    src = st["from"]
    _check(src in relations,
           f"step {i}: write references unknown/forward relation {src!r}")
    if st["effect"] == "delete":
        _check("values" not in st, f"step {i}: delete writes no values")
    values = st.get("values", {})
    _check(isinstance(values, dict) and len(values) <= MAX_VALUE_COLS,
           f"step {i}: values must be an object of <= {MAX_VALUE_COLS} columns")
    for col, expr in values.items():
        _check_simple_name(col, i, "column name")
        _check_rel_value_expr(expr, relations[src], i, f"value for {col!r}",
                              counter=[0])
    _check_rel_value_expr(st["target"], relations[src], i, "write target",
                          counter=[0], target=True)
    out = {"op": "write", "effect": st["effect"], "table": st["table"],
           "from": src, "target": st["target"]}
    if "values" in st:
        out["values"] = values
    return out


def _validate_guard(st, i, relations):
    keys = set(st)
    _check(keys <= {"op", "test", "relation", "field", "eq", "ge", "le"}
           and {"op", "test", "relation"} <= keys,
           f"step {i}: guard shape")
    _check(st["test"] in ("row_count", "int_sum"), f"step {i}: guard test")
    rel = st["relation"]
    _check(rel in relations,
           f"step {i}: guard references unknown/forward relation {rel!r}")
    if st["test"] == "row_count":
        _check("field" not in st, f"step {i}: row_count guard takes no field")
    else:
        _check(isinstance(st.get("field"), str) and st["field"],
               f"step {i}: int_sum needs field")
        _check_rel_field(st["field"], relations[rel], i, "guard")
    ops = [k for k in ("eq", "ge", "le") if k in st]
    _check(ops, f"step {i}: guard needs at least one of eq/ge/le")
    for k in ops:
        _check(_is_int(st[k]), f"step {i}: guard {k} must be int")
    return copy.deepcopy(st)


def validate_relational_program(program):
    """Validate a managed-relational/v1 program; return a normalized copy.

    Raises ProtocolError/LimitError.  Does not touch any database.
    """
    _check(isinstance(program, dict), "program must be an object")
    extra = set(program) - _PROG_KEYS
    _check(not extra, f"program top-level keys must be logical_batch_id/"
                      f"schema/steps/partition (extra {sorted(extra)})")
    _check(_PROG_REQUIRED <= set(program),
           "program needs logical_batch_id/schema/steps")
    _check(isinstance(program["logical_batch_id"], str)
           and 1 <= len(program["logical_batch_id"]) <= 200,
           "logical_batch_id must be a 1..200 char string")
    _check(program["schema"] == RELATIONAL_SCHEMA,
           f"unsupported schema {program['schema']!r}")
    steps = program["steps"]
    _check(isinstance(steps, list) and 1 <= len(steps) <= MAX_STEPS,
           f"steps must be a list of 1..{MAX_STEPS}")
    prog = copy.deepcopy(program)
    _check_partition(prog.get("partition"))
    relations = {}      # alias -> relation schema (open/closed/join)
    read_aliases = {}   # read alias -> table (for predicate field refs)
    steps_out = []
    for i, st in enumerate(prog["steps"]):
        _check(isinstance(st, dict), f"step {i}: must be an object")
        op = st.get("op")
        if op == "read":
            steps_out.append(_validate_read(st, i, relations, read_aliases))
        elif op == "join":
            steps_out.append(_validate_join(st, i, relations))
        elif op == "project":
            steps_out.append(_validate_project(st, i, relations))
        elif op == "group":
            steps_out.append(_validate_group(st, i, relations))
        elif op == "write":
            steps_out.append(_validate_write(st, i, relations))
        elif op == "guard":
            steps_out.append(_validate_guard(st, i, relations))
        else:
            raise ProtocolError(f"step {i}: unknown op {op!r}")
    prog["steps"] = steps_out
    return prog
