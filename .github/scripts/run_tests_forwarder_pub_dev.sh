#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

# Keep an explicitly supplied secret ahead of values in local dotenv fixtures.
PRIVATE_KEY_FROM_ENV="${VBASE_COMMITMENT_SERVICE_PRIVATE_KEY:-}"

set -a
source "${REPO_ROOT}/config/.env.forwarder.pub.dev"
set +a

if [[ -n "${PRIVATE_KEY_FROM_ENV}" ]]; then
    export VBASE_COMMITMENT_SERVICE_PRIVATE_KEY="${PRIVATE_KEY_FROM_ENV}"
fi

: "${VBASE_COMMITMENT_SERVICE_PRIVATE_KEY:?VBASE_COMMITMENT_SERVICE_PRIVATE_KEY must be supplied by the environment or a secret manager}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN=python3
    elif command -v python >/dev/null 2>&1; then
        PYTHON_BIN=python
    else
        echo "Neither python3 nor python was found." >&2
        exit 1
    fi
fi

"${PYTHON_BIN}" "${REPO_ROOT}/.github/scripts/run_tests_forwarder_pub_dev.py"
