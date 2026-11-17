#!/usr/bin/env bash
# Reproduce everything: unit tests, the independent-oracle sweep and the drills.
set -euo pipefail
cd "$(dirname "$0")"
python3 -m unittest discover -s tests -v
python3 drills/run_drills.py
python3 drills/run_constraint_drills.py
python3 drills/run_relational_drills.py
