#!/usr/bin/env bash
# ─── HELIX Smoke Tests ────────────────────────────────────────────────────────
# Run the smoke suite — intended for PR gates.
# Target: < 10 minutes total.
#
# Usage:
#   ./scripts/run_smoke.sh --cluster-ip=10.0.0.100
#   CLUSTER_IP=10.0.0.100 ./scripts/run_smoke.sh
#
# Required env vars:
#   HELIOS_API_KEY    Cohesity Helios API key
#   CLUSTER_IP        Cluster management IP (or pass --cluster-ip)
#
# Optional env vars:
#   CLUSTER_ID        Cluster ID for accessClusterId header
#   SSH_USER          SSH username (default: cohesity)
#   SSH_KEY_PATH      SSH key path (default: ~/.ssh/id_rsa)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

# Defaults
CLUSTER_IP="${CLUSTER_IP:-}"
CLUSTER_ID="${CLUSTER_ID:-}"
ENV="${HELIX_ENV:-lab}"
EXTRA_ARGS="${@}"

echo "═══════════════════════════════════════════"
echo "  HELIX Smoke Suite"
echo "  Cluster: ${CLUSTER_IP:-<not set>}"
echo "  Env:     ${ENV}"
echo "═══════════════════════════════════════════"

# Ensure allure results dir exists
mkdir -p allure-results

pytest \
  -m smoke \
  --cluster-ip="${CLUSTER_IP}" \
  --cluster-id="${CLUSTER_ID}" \
  --env="${ENV}" \
  --tb=short \
  -ra \
  --timeout=120 \
  --alluredir=allure-results \
  -v \
  ${EXTRA_ARGS} \
  tests/smoke/

EXIT_CODE=$?

echo ""
if [ ${EXIT_CODE} -eq 0 ]; then
  echo "✓ Smoke suite PASSED"
else
  echo "✗ Smoke suite FAILED (exit code ${EXIT_CODE})"
  echo "  View report: allure serve allure-results"
fi

exit ${EXIT_CODE}
