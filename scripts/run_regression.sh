#!/usr/bin/env bash
# ─── HELIX Regression Suite ───────────────────────────────────────────────────
# Run the full regression suite: functional + integration tests.
# Excludes performance and chaos (those require dedicated scheduling).
#
# Usage:
#   ./scripts/run_regression.sh --cluster-ip=10.0.0.100
#   ./scripts/run_regression.sh --cluster-ip=10.0.0.100 -k "smb or nfs"  # filter tests
#
# Typical runtime: 30-60 minutes
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

CLUSTER_IP="${CLUSTER_IP:-}"
CLUSTER_ID="${CLUSTER_ID:-}"
WORKERS="${WORKERS:-4}"
ENV="${HELIX_ENV:-lab}"
EXTRA_ARGS="${@}"

echo "═══════════════════════════════════════════"
echo "  HELIX Regression Suite"
echo "  Cluster:  ${CLUSTER_IP:-<not set>}"
echo "  Workers:  ${WORKERS}"
echo "  Env:      ${ENV}"
echo "═══════════════════════════════════════════"

mkdir -p allure-results

pytest \
  -m "regression and not perf and not chaos" \
  --cluster-ip="${CLUSTER_IP}" \
  --cluster-id="${CLUSTER_ID}" \
  --env="${ENV}" \
  -n "${WORKERS}" \
  --tb=short \
  -ra \
  --timeout=300 \
  --alluredir=allure-results \
  -v \
  ${EXTRA_ARGS} \
  tests/

EXIT_CODE=$?

echo ""
if [ ${EXIT_CODE} -eq 0 ]; then
  echo "✓ Regression suite PASSED"
else
  echo "✗ Regression suite FAILED (exit code ${EXIT_CODE})"
  echo "  View report: allure serve allure-results"
fi

exit ${EXIT_CODE}
