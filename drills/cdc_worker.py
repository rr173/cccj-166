"""Subprocess worker for the CDC crash/concurrency drill.

Invoked as a real OS process (``python3 -I drills/cdc_worker.py ...``) so an
``os._exit`` genuinely kills the process between durability points instead of
being simulated in-process.  It never imports the drill code itself; it only
drives the public package API.

Commands
--------
receive DB SOURCE FILE
    stage one JSONL fragment file (may itself be truncated)
advance DB SOURCE [--crash POINT]
    advance the source, optionally hard-exiting at a named crash point:
      after_staging_before_commit / after_commit_before_response
status DB SOURCE
    print the source status JSON
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from revoke_ledger.ledger import Ledger  # noqa: E402


def main(argv):
    cmd = argv[1]
    db_path = argv[2]
    source_id = argv[3]
    L = Ledger(db_path)
    cdc = L.cdc()
    if cmd == "receive":
        result = cdc.receive_file(argv[4])
        print(json.dumps({"ok": True, "staged": result["staged"],
                          "duplicate": result["duplicate"]}))
    elif cmd == "advance":
        point = None
        if len(argv) > 4 and argv[4] == "--crash":
            point = argv[5]
        if point is not None:
            cdc.crash_points[point] = True
        # A concurrent advancer may hold the source lock; the loser retries and
        # then reads the winner's durable decision instead of failing.
        import time
        last = None
        for attempt in range(20):
            try:
                result = cdc.advance(source_id)
                print(json.dumps({"ok": True, "result": result,
                                  "attempts": attempt + 1}))
                break
            except Exception as exc:
                last = exc
                if type(exc).__name__ != "CdcBusyError":
                    raise
                time.sleep(0.1)
        else:
            raise last
    elif cmd == "status":
        print(json.dumps(cdc.source_status(source_id), sort_keys=True))
    else:
        print(json.dumps({"ok": False, "error": f"unknown command {cmd}"}))
        return 2
    L.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except Exception as exc:  # surface a structured failure to the parent
        print(json.dumps({"ok": False, "error": type(exc).__name__,
                          "message": str(exc)}))
        sys.exit(1)
