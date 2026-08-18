#!/bin/bash

set -a
source config/.env.forwarder.pub.dev
set +a
: "${VBASE_COMMITMENT_SERVICE_PRIVATE_KEY:?VBASE_COMMITMENT_SERVICE_PRIVATE_KEY must be supplied by the environment or a secret manager}"

python3 -m unittest vbase.tests.test_vbase_client
python3 -m unittest vbase.tests.test_indexing_service
