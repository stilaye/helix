#!/usr/bin/env bash
# ─── HELIX Chaos Suite ────────────────────────────────────────────────────────
# Run fault injection tests. ONLY on --env=lab.
# Never run chaos tests against staging or production clusters.
#
# Usage:
#   ./scripts/run_chaos.sh --cluster-ip=10.0.0.100
#
# SAFETY: Script will exit immediately if HELIX_ENV is not "lab".
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

CLUSTER_IP="${CLUSTER_IP:-}"
CLUSTER_ID="${CLUSTER_ID:-}"
ENV="${HELIX_ENV:-lab}"
EXTRA_ARGS="${@}"

# Hard safety check
if [ "${ENV}" != "lab" ]; then
  echo "ERROR: Chaos tests require HELIX_ENV=lab (current: ${ENV})"
  echo "       Never run fault injection on staging or production clusters."
  exit 1
fi

echo "═══════════════════════════════════════════"
echo "  HELIX Chaos Suite"
echo "  Cluster: ${CLUSTER_IP:-<not set>}"
echo "  Env:     ${ENV} (lab-only)"
echo "  ⚠  Will inject faults: kill nodes, partition networks, break disks"
echo "═══════════════════════════════════════════"
echo ""
echo "Press Ctrl+C within 5 seconds to abort..."
sleep 5

mkdir -p allure-results

pytest \
  -m "chaos and destructive" \
  --cluster-ip="${CLUSTER_IP}" \
  --cluster-id="${CLUSTER_ID}" \
  --env="${ENV}" \
  --tb=long \
  -ra \
  --timeout=300 \
  --alluredir=allure-results \
  -v \
  -p no:randomly \
  ${EXTRA_ARGS} \
  tests/chaos/

EXIT_CODE=$?

echo ""
echo "Post-chaos cluster health check..."
echo "  Verify cluster is healthy: ./scripts/run_smoke.sh --cluster-ip=${CLUSTER_IP}"

exit ${EXIT_CODE}
