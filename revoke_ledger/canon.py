"""Canonical JSON encoding and content ids.

All hashes over structured data use canonical JSON (sorted keys, no whitespace,
ASCII) so that evidence, programs and plan payloads are byte-stable.
"""

import hashlib
import json

MAX_INT = 2 ** 63


def _json_default(obj):
    # Deterministic sets sorted by their canonical encoding.
    if isinstance(obj, (set, frozenset)):
        items = sorted(canon(obj))
        return items
    raise TypeError(f"not JSON: {type(obj)!r}")


def canon(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      default=_json_default)


def sha256(obj_or_bytes):
    if isinstance(obj_or_bytes, str):
        data = obj_or_bytes.encode("utf-8")
    elif isinstance(obj_or_bytes, (bytes, bytearray)):
        data = bytes(obj_or_bytes)
    else:
        data = canon(obj_or_bytes).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def content_id(prefix, payload):
    return f"{prefix}-{sha256(payload)[:16]}"
