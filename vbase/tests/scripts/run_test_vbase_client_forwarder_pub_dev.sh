#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
bash "${REPO_ROOT}/.github/scripts/run_tests_forwarder_pub_dev.sh" vbase.tests.test_vbase_client
