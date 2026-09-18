# Development

## Setup

Python 3.11 is the minimum supported version. CI validates Python 3.11 and 3.12,
and generated lock files are maintained with Python 3.11.

```bash
python -m pip install --require-hashes -r requirements/dev.txt
python -m pip install --no-deps --no-build-isolation -e .
pre-commit install
```

## Linting And Formatting

```bash
black vbase/
isort vbase/
pylint vbase/
pre-commit run --all-files
```

`vbase/__init__.py` is intentionally ordered by public export grouping instead
of alphabetically, so isort skips it.

## Tests

Tests use `unittest`. Integration-style tests require a running Ethereum node,
forwarder, or localhost commitment service.

```bash
./vbase/tests/scripts/run_tests_localhost.sh
./vbase/tests/scripts/run_tests_forwarder_pub_dev.sh
source config/.env.localhost
python3 -m unittest discover -s vbase/tests
```

The public dev forwarder runner generates a fresh ephemeral signer key and
address for every invocation. This prevents prior test history and nonce state
from leaking into later runs; only `VBASE_API_KEY` must be supplied externally.

Forwarder Problem Details parsing follows the public contract in
`docs/problem-details.schema.json`. The RFC standard members plus the vBase
`code` extension must be valid before the SDK raises `ProblemDetailsError`.
The required `type` and optional `instance` members must be valid RFC 3986 URI
references; `instance` may be omitted. The optional `details` member may also be
omitted, but when present it must be a JSON object. Malformed documents remain
ordinary `requests.HTTPError` instances. Forwarder implementations and
downstream applications must conform to this public schema.

## Environment

Test environment files live under `config/`:

- `config/.env.localhost`
- `config/.env.forwarder.localhost`
- `config/.env.forwarder.pub.dev`

Important environment variables include `VBASE_COMMITMENT_SERVICE_NODE_RPC_URL`,
`VBASE_COMMITMENT_SERVICE_ADDRESS`, and `VBASE_COMMITMENT_SERVICE_PRIVATE_KEY`.
