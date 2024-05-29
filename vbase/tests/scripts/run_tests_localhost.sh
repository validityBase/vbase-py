#!/bin/bash

set -a
source config/.env.localhost
set +a

python3 -m unittest discover -s vbase/tests
