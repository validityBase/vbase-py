# Development

## Setup

Python 3.11 is the minimum supported version. CI validates Python 3.11 through
3.13, and generated lock files are maintained with Python 3.11 for CI parity.

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
bash vbase/tests/scripts/run_tests_localhost.sh
bash .github/scripts/run_tests_forwarder_pub_dev.sh
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

## Sensitive Logging

- Never log complete API keys, private keys, access tokens, credentials, or RPC
  URLs that may contain provider credentials.
- API keys needed for diagnostic correlation use `vbase.utils.log.mask_api_key`,
  which retains the first and last eight characters only for keys of at least
  32 characters. Shorter keys are fully redacted.
- Log only whether private keys and RPC URLs are configured. Public contract
  addresses may be logged directly.

- Indexing descriptors may contain credentials: log only the commitment service
  count, never the descriptor or individual service configuration.
- Forwarder, RPC, and indexing error text may contain credential-bearing URLs.
  Modules that log it attach `vbase.utils.log.RedactUrlsFilter` to their
  logger, including loggers passed to `retry_call`. The filter applies
  `vbase.utils.log.redact_urls`, which keeps each URL's scheme and host and
  redacts userinfo, paths, queries, and fragments. Trailing punctuation after
  a path, query, or fragment is redacted too, because it can belong to a
  credential; it is kept only after an already-redacted path or from the
  closing quote or bracket for the character before the URL. In free text,
  whitespace can appear in URL credentials, so text from `://` across
  whitespace to the first `@` that precedes any `/`, `?`, or `#` keeps only
  the scheme. Do not log service objects.
- URL redaction is best effort. It covers the credential forms expected in
  SDK messages and in error text from requests, urllib3, web3, and the vBase
  forwarder. Unencoded credentials that combine whitespace with `@`, `/`, `?`,
  or `#` are not expected and may be only partly redacted. If such a case is
  identified, handle it separately.
- Before placing a configured URL, such as a node RPC URL, in a log or
  exception message, reduce it with `vbase.utils.log.redact_url`. It treats
  the whole value as one URL, so it keeps the host even when requests-accepted
  whitespace appears in the URL's credentials. Use `redact_urls` for other
  text that may contain URLs.
- Errors from requests and urllib3 include the request URL. The SDK redacts
  their messages in place with `vbase.utils.log.redact_exception`, which also
  covers wrapped errors, the exceptions in an exception group, and the
  `__cause__` and `__context__` chain, so tracebacks do not show URL
  credentials. It also redacts the other text that tracebacks show:
  exception notes, and the `strerror`, `filename`, and `filename2`
  attributes from which an `OSError` builds its message. The exceptions keep
  their type and other attributes, such as `request` and `response`, so
  callers can catch and inspect them.
- An exception passed to another process, for example from a
  `concurrent.futures.ProcessPoolExecutor` or `multiprocessing` worker to its
  parent, is serialized with `pickle` and rebuilt in the receiving process.
  Most exceptions are rebuilt from their arguments, which are already
  redacted. urllib3 errors are rebuilt from their `url` and `_message`
  attributes instead, and `json.JSONDecodeError` from its `msg` attribute,
  so `redact_exception` redacts those attributes too.
- Every node request that the SDK sends, including indexing queries, goes
  through the `make_request` method of the HTTP provider that
  `Web3HTTPCommitmentService` installs. The SDK does not send JSON-RPC batch
  requests, which use another provider method. The provider redacts the
  errors raised by the request. web3 raises errors for JSON-RPC error
  responses, such as `Web3RPCError`, after the provider returns the
  response, and includes the response text in them, so the provider also
  redacts URLs in the response outside its `result`. Forwarder request
  errors are redacted where they are caught, and `FailoverIndexingService`
  redacts each service error before logging or chaining it.
- Dependency logs include request URLs: the debug logs of
  `web3.providers.HTTPProvider` and `urllib3.connectionpool`, and some
  urllib3 warnings, such as the warning for response headers that it cannot
  parse. The SDK does not change dependency loggers. Applications that
  enable these logs can add `vbase.utils.log.RedactUrlsFilter` to their
  handlers. For a record with exception information, the filter also
  redacts the exception in place with `redact_exception`, so the traceback
  that a formatter adds after the message is redacted too.

## Public Dev Tests

The public-dev forwarder runner requires `VBASE_API_KEY` from the environment
or a secret manager. It generates a fresh signer private key for each invocation
and does not use a committed or externally supplied signer key.

The indexing tests use unique object CIDs for global object lookups so old
public-chain test data does not accumulate in those queries.
The [`TestIndexingService` setup](../../vbase/tests/test_indexing_service.py)
reads `VBASE_TEST_INDEXING_N_LAST_BLOCKS` to configure its block lookback.
[`config/.env.forwarder.pub.dev`](../../config/.env.forwarder.pub.dev) sets it
for public-dev runs. See
[Indexing and Node Requirements](../../README.md#indexing-and-node-requirements)
for endpoint requirements and event visibility.
