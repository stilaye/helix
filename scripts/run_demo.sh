#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_demo.sh — interview demo: runs all HELIX tests against mock infrastructure
#
# No real Cohesity cluster needed. Creates a virtualenv on first run.
#
# Usage:
#   ./scripts/run_demo.sh
#   ./scripts/run_demo.sh -k test_smoke          # smoke only
#   ./scripts/run_demo.sh -k test_protocols      # protocol tests only
#   ./scripts/run_demo.sh -k test_performance    # perf + baseline regression
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
VENV="$REPO_ROOT/.venv"

cd "$REPO_ROOT"

# ── Create virtualenv if it doesn't exist ────────────────────────────────────
if [ ! -d "$VENV" ]; then
    echo "Creating virtual environment at .venv ..."
    python3 -m venv "$VENV"
fi

# ── Activate ─────────────────────────────────────────────────────────────────
source "$VENV/bin/activate"

# ── Install dependencies ──────────────────────────────────────────────────────
echo "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
pip install --quiet -e .

echo ""
echo "======================================================================"
echo "  HELIX — Demo Test Suite"
echo "  31 tests: smoke | protocols (SMB/NFS/S3) | performance baselines"
echo "======================================================================"
echo ""

python -m pytest tests/demo/ -v --tb=short "$@"
