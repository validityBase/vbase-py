#!/bin/bash

set -a
source .env.localhost
set +a

python3 -m unittest discover -s vbase/tests
