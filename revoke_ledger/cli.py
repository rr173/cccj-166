"""Command-line driver for the ledger (thin, no business logic).

    python3 -m revoke_ledger.cli init       DB SEED.json [CONSTRAINTS.json]
    python3 -m revoke_ledger.cli constraints DB [CONSTRAINTS.json]
    python3 -m revoke_ledger.cli explain    DB LOGICAL_ID
    python3 -m revoke_ledger.cli explain-derived DB LOGICAL_ID
    python3 -m revoke_ledger.cli explain-rev DB REVOCATION_OR_PLAN_ID
    python3 -m revoke_ledger.cli ingest     DB PROGRAM.json
    python3 -m revoke_ledger.cli run        DB LOGICAL_ID
    python3 -m revoke_ledger.cli plan       DB REVOCATION_ID TARGET_ID
    python3 -m revoke_ledger.cli apply      DB PLAN_ID
    python3 -m revoke_ledger.cli result     DB REVOCATION_ID
    python3 -m revoke_ledger.cli show       DB [GENERATION]
    python3 -m revoke_ledger.cli history    DB
"""

import json
import sys

from .errors import BatchAbort, LedgerError
from .ledger import Ledger


def _load(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _dump(obj):
    print(json.dumps(obj, indent=1, sort_keys=True))


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    cmd, args = argv[1], argv[2:]
    L = None
    try:
        if cmd == "init":
            db, seed = args[0], args[1]
            cfg_path = args[2] if len(args) > 2 else None
            L = Ledger(db)
            if cfg_path:
                _dump(L.install_constraints(_load(cfg_path)))
            rows = _load(seed)
            rows = {t: {int(k): v for k, v in m.items()} for t, m in rows.items()}
            try:
                _dump(L.seed(rows))
            except BatchAbort as exc:
                _dump({"error": "seed_rejected", "abort": exc.as_dict()})
                return 1
        elif cmd == "constraints":
            db = args[0]
            L = Ledger(db)
            if len(args) > 1:
                _dump(L.install_constraints(_load(args[1])))
            else:
                _dump(L.constraints_view())
        elif cmd == "explain":
            L = Ledger(args[0])
            _dump(L.explain_batch(args[1]))
        elif cmd == "explain-derived":
            L = Ledger(args[0])
            _dump(L.explain_derived(args[1]))
        elif cmd == "explain-rev":
            L = Ledger(args[0])
            _dump(L.explain_revocation(args[1]))
        elif cmd == "ingest":
            db, prog = args
            L = Ledger(db)
            _dump(L.ingest(_load(prog)))
        elif cmd == "run":
            db, lid = args
            L = Ledger(db)
            _dump(L.run_ingested(lid))
        elif cmd == "plan":
            db, rid, tgt = args
            L = Ledger(db)
            _dump(L.plan_revocation(rid, tgt))
        elif cmd == "apply":
            db, pid = args
            L = Ledger(db)
            _dump(L.apply_plan(pid))
        elif cmd == "result":
            db, rid = args
            L = Ledger(db)
            _dump(L.revocation_result(rid))
        elif cmd == "show":
            db = args[0]
            gen = int(args[1]) if len(args) > 1 else None
            L = Ledger(db)
            _dump(L.snapshots(gen))
        elif cmd == "history":
            L = Ledger(args[0])
            _dump(L.history())
        else:
            print(__doc__)
            return 2
    except LedgerError as exc:
        _dump({"error": type(exc).__name__, "message": str(exc),
               "code": getattr(exc, "code", None)})
        return 1
    finally:
        if L is not None:
            L.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
