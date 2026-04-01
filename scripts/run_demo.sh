#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_demo.sh — interview demo: runs all HELIX tests against mock infrastructure
#
# No real Cohesity cluster needed. All tests run locally using mock fixtures.
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

cd "$REPO_ROOT"

# Ensure the package is importable
if ! python3 -c "import helix" 2>/dev/null; then
    echo "Installing helix package..."
    pip3 install -e ".[test]" --quiet
fi

echo "======================================================================"
echo "  HELIX — Demo Test Suite"
echo "  31 tests: smoke | protocols (SMB/NFS/S3) | performance baselines"
echo "======================================================================"
echo ""

python3 -m pytest tests/demo/ -v --tb=short "$@"
