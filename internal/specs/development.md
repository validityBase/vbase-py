# Development

## Setup

Use Python 3.11 for CI parity. The repository still declares package support for
Python 3.8+, but generated lock files are maintained with Python 3.11.

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
bash vbase/tests/scripts/run_tests_forwarder_pub_dev.sh
source config/.env.localhost
python3 -m unittest discover -s vbase/tests
```

Forwarder Problem Details parsing follows the SDK consumer profile in
`docs/problem-details.schema.json`. The profile deliberately remains
forward-compatible; the Forwarder owns its producer-specific error catalog.
The SDK checks the standard members' structure plus the vBase `code` extension
before raising `ProblemDetailsError`. Codes contain only uppercase ASCII
letters, digits, and underscores and are limited to 64 characters. The optional
`details` member may be omitted, but when present it must be a JSON object.
Responses failing these checks remain ordinary `requests.HTTPError` instances.

Producers must supply RFC 3986 URI references for `type` and `instance`;
`instance` may be omitted, but not JSON null. The SDK treats them as identifiers
and applies lightweight checks: ASCII strings without raw whitespace/control
characters, well-formed percent escapes, and successful `urlsplit` parsing.
It does not certify full URI syntax or URL safety. Schema `uri-reference`
formats are producer-contract annotations, not exhaustive runtime assertions.
Do not add a URI-validation dependency or a custom authority/IP/port grammar.
Parsing uses a lowercase validation copy for the IPvFuture marker; identifiers
are preserved unchanged and are never dereferenced. Tests cover both fields,
including the deliberate boundary between sanity checks and full validation.

## Environment

Test environment files live under `config/`:

- `config/.env.localhost`
- `config/.env.forwarder.localhost`
- `config/.env.forwarder.pub.dev`

Important environment variables include `VBASE_COMMITMENT_SERVICE_NODE_RPC_URL`,
`VBASE_COMMITMENT_SERVICE_ADDRESS`, and `VBASE_COMMITMENT_SERVICE_PRIVATE_KEY`.
The public-dev forwarder runner requires `VBASE_API_KEY` from the environment
or a secret manager. It generates a fresh signer private key for each invocation
and does not use a committed or externally supplied signer key.
The indexing tests use unique object CIDs for global object lookups so old
public-chain test data does not accumulate in those queries.
