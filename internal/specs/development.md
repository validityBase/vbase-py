# Development

## Setup

Use Python 3.11 for CI parity. The repository still declares package support for
Python 3.8+, but generated lock files are maintained with Python 3.11.

```bash
python -m pip install --require-hashes -r requirements-dev.txt
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

## Environment

Test environment files live under `config/`:

- `config/.env.localhost`
- `config/.env.forwarder.localhost`
- `config/.env.forwarder.pub.dev`

Important environment variables include `VBASE_COMMITMENT_SERVICE_NODE_RPC_URL`,
`VBASE_COMMITMENT_SERVICE_ADDRESS`, and `VBASE_COMMITMENT_SERVICE_PRIVATE_KEY`.
