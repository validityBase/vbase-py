#!/bin/bash

set -a
source config/.env.localhost
set +a

python3 -m unittest vbase.tests.test_vbase_client
