#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
bash "${SCRIPT_DIR}/../../../.github/scripts/run_tests_forwarder_pub_dev.sh" vbase.tests.test_vbase_client
