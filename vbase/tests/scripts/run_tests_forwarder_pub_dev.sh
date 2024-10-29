#!/bin/bash

set -a
source config/.env.forwarder.pub.dev
set +a

python3 -m unittest vbase.tests.test_vbase_client
python3 -m unittest vbase.tests.test_indexing_service
