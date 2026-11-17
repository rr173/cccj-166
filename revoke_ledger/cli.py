"""Command-line driver for the ledger (thin, no business logic).

    python3 -m revoke_ledger.cli init   DB SEED.json
    python3 -m revoke_ledger.cli ingest DB PROGRAM.json
    python3 -m revoke_ledger.cli run    DB LOGICAL_ID
    python3 -m revoke_ledger.cli plan   DB REVOCATION_ID TARGET_ID
    python3 -m revoke_ledger.cli apply  DB PLAN_ID
    python3 -m revoke_ledger.cli result DB REVOCATION_ID
    python3 -m revoke_ledger.cli show   DB [GENERATION]
    python3 -m revoke_ledger.cli history DB
"""

import json
import sys

from .errors import LedgerError
from .ledger import Ledger


def _load(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    cmd, args = argv[1], argv[2:]
    L = None
    try:
        if cmd == "init":
            db, seed = args
            L = Ledger(db)
            rows = _load(seed)
            rows = {t: {int(k): v for k, v in m.items()} for t, m in rows.items()}
            print(json.dumps(L.seed(rows), indent=1))
        elif cmd == "ingest":
            db, prog = args
            L = Ledger(db)
            print(json.dumps(L.ingest(_load(prog)), indent=1))
        elif cmd == "run":
            db, lid = args
            L = Ledger(db)
            print(json.dumps(L.run_ingested(lid), indent=1))
        elif cmd == "plan":
            db, rid, tgt = args
            L = Ledger(db)
            print(json.dumps(L.plan_revocation(rid, tgt), indent=1))
        elif cmd == "apply":
            db, pid = args
            L = Ledger(db)
            print(json.dumps(L.apply_plan(pid), indent=1))
        elif cmd == "result":
            db, rid = args
            L = Ledger(db)
            print(json.dumps(L.revocation_result(rid), indent=1))
        elif cmd == "show":
            db = args[0]
            gen = int(args[1]) if len(args) > 1 else None
            L = Ledger(db)
            print(json.dumps(L.snapshots(gen), indent=1, sort_keys=True))
        elif cmd == "history":
            L = Ledger(args[0])
            print(json.dumps(L.history(), indent=1))
        else:
            print(__doc__)
            return 2
    except LedgerError as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc),
                          "code": getattr(exc, "code", None)}), file=sys.stderr)
        return 1
    finally:
        if L is not None:
            L.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
