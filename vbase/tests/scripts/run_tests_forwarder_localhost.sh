#!/bin/bash

set -a
source config/.env.forwarder.localhost
set +a

python3 -m unittest vbase.tests.test_vbase_client
python3 -m unittest vbase.tests.test_indexing_service
python3 -m unittest vbase.tests.test_vbase_dataset
python3 -m unittest vbase.tests.test_vbase_dataset_async
python3 -m unittest vbase.tests.test_vbase_dataset_bootstrap
python3 -m unittest vbase.tests.test_sim
