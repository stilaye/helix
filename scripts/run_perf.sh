#!/usr/bin/env bash
# ─── HELIX Performance Suite ──────────────────────────────────────────────────
# Run performance benchmarks with optional baseline update.
#
# Usage:
#   ./scripts/run_perf.sh --cluster-ip=10.0.0.100
#   ./scripts/run_perf.sh --cluster-ip=10.0.0.100 --update-baselines
#
# --update-baselines flag records new baseline values without failing on regression.
# Use this after intentional performance changes (e.g., hardware upgrade, tuning).
#
# Typical runtime: 45-90 minutes
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

CLUSTER_IP="${CLUSTER_IP:-}"
CLUSTER_ID="${CLUSTER_ID:-}"
UPDATE_BASELINES="${UPDATE_BASELINES:-false}"
ENV="${HELIX_ENV:-lab}"
EXTRA_ARGS="${@}"

UPDATE_FLAG=""
if [ "${UPDATE_BASELINES}" = "true" ]; then
  UPDATE_FLAG="--update-baselines"
  echo "⚠  Baseline update mode: regression failures suppressed"
fi

echo "═══════════════════════════════════════════"
echo "  HELIX Performance Suite"
echo "  Cluster: ${CLUSTER_IP:-<not set>}"
echo "  Update baselines: ${UPDATE_BASELINES}"
echo "  Env:     ${ENV}"
echo "═══════════════════════════════════════════"

mkdir -p allure-results

pytest \
  -m perf \
  --cluster-ip="${CLUSTER_IP}" \
  --cluster-id="${CLUSTER_ID}" \
  --env="${ENV}" \
  ${UPDATE_FLAG} \
  --tb=short \
  -ra \
  --timeout=600 \
  --alluredir=allure-results \
  -v \
  ${EXTRA_ARGS} \
  tests/performance/

EXIT_CODE=$?

if [ "${UPDATE_BASELINES}" = "true" ] && [ ${EXIT_CODE} -eq 0 ]; then
  echo ""
  echo "Baselines updated. Consider committing baselines/ directory:"
  echo "  git add baselines/ && git commit -m 'chore: update performance baselines'"
fi

exit ${EXIT_CODE}
