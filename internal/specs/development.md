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
  the scheme. A malformed URL whose host part may hold credentials also keeps
  only its scheme. An `@` after the host part means that an unencoded `/`,
  `?`, or `#` in the credentials ended the host part early. A port that is
  not a number from 0 to 65535 may be a password whose `@host` is missing.
  The port is all the text after the first `:` of a host name or IPv4
  address, or after the `]` of a bracketed IPv6 address. In free text,
  punctuation that can follow a URL is ignored after the port's digits when
  the host part ends the URL, as in `https://rpc.example:8545: refused`. In
  a configured URL, or when a path, query, or fragment follows, the whole
  port must be digits. Do not log service objects.
- Before placing a configured URL, such as a node RPC URL, in a log or
  exception message, reduce it with `vbase.utils.log.redact_url`. It treats
  the whole value as one URL, so it keeps the host even when requests-accepted
  whitespace appears in the URL's credentials. It applies the same rule for
  malformed URLs. Use `redact_urls` for other text that may contain URLs.
- URL redaction recognizes a URL by its contiguous `scheme://` and ends it at
  whitespace. requests fails on a URL that is not an `http` or `https` URL
  with a host, or that has a tab or newline in its scheme, host, or port, and
  shows the URL in its error message as given. The message then shows the
  URL without a scheme, with escapes such as `https:\t//`, or split by
  whitespace, so redaction can miss its credentials. `urlsplit` removes tabs
  and newlines before parsing, so it cannot detect these URLs by itself.
  `ForwarderCommitmentService` therefore rejects a `forwarder_url` that is
  not an `http` or `https` URL with a host, or that has whitespace or control
  characters after any leading whitespace, which requests removes. The
  `ValueError` omits the value. The forwarder authenticates with the
  `X-API-KEY` header, so its URL needs no credentials or whitespace. A
  malformed node URL fails the connection check of
  `Web3HTTPCommitmentService`, because web3 handles the requests error, and
  the resulting `ConnectionError` shows the URL redacted as one value, or as
  `[REDACTED]` when it has no `scheme://`.
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
  `Web3HTTPCommitmentService` installs. The provider redacts the errors
  raised by the request. web3 raises errors for JSON-RPC error responses,
  such as `Web3RPCError`, after the provider returns the response, and
  includes the response text in them, so the provider also redacts URLs in
  the response outside its `result`. Forwarder request errors are redacted
  where they are caught, and `FailoverIndexingService` redacts each service
  error before logging or chaining it.

### URL Redaction Limits

URL redaction protects the credentials in URLs that the SDK is configured
with, such as node and forwarder URLs, and URLs in error text from requests,
urllib3, web3, and the forwarder. The limits below are by design, and
reviews should not report them as defects. Report a finding only when a
change affects one of these limits, or when it shows a path that the SDK or
those dependencies take that leaves such a credential in a message,
traceback, or log. A change that affects a
limit updates this list and its short copy for Copilot code review in
[`.github/instructions/url-redaction.instructions.md`](../../.github/instructions/url-redaction.instructions.md).

- **Free-text matching is best effort.** `redact_urls` covers the credential
  forms expected in SDK messages and in error text from requests, urllib3,
  web3, and the vBase forwarder. Unencoded credentials that combine
  whitespace with `@`, `/`, `?`, or `#` are not expected and may be only
  partly redacted. When a quote or bracket comes before a URL, the
  punctuation from the matching closing character onward is kept as
  surrounding text, even if that character ends a credential, because the
  text does not show which it is. Text is recognized as a URL only by its
  scheme, so a URL without one, such as `rpc.example/v2/key`, is not
  redacted in free text. Malformed configured forwarder and node URLs are
  covered by the rule above.
- **Hosts and numeric ports are kept.** They identify the failing endpoint,
  so a credential placed in a host, such as a key used as a subdomain, is
  not redacted. A URL with an `@` after its host part loses its host, even
  when the `@` is in a query, such as `?email=a@b`. The text in its host
  position may be credentials. A URL with credentials but no host, such as
  `https://user:1234` or `https://user:p@ss`, has the form of a valid URL
  with host `user` and port `1234`, or user `user`, password `p`, and host
  `ss`. Its host part is therefore kept. Such a URL cannot connect, and the
  text cannot show that it is a credential. In free text, punctuation after
  such a port at the end of a URL, as in `https://user:1234!!!`, is read as
  surrounding text. In a configured URL, the port must be all digits, so
  `redact_url` keeps only the scheme of that value.
- **Attributes that callers use keep their values.** The `request` and
  `response` of a requests error keep the full URL and the response body,
  `ProblemDetailsError` attributes keep the server's values,
  `JSONDecodeError.doc` keeps the decoded text, and the node provider keeps
  the configured URL in `endpoint_uri` because it sends requests to it.
  Redacting them would break error handling and retries. Do not log these
  attributes, or provider or database engine objects.
- **Exception text outside the redacted attributes is kept.**
  `redact_exception` redacts arguments, notes, the `OSError` attributes, and
  the attributes that urllib3 errors and `JSONDecodeError` are rebuilt from.
  An exception group's title is its read-only `message` attribute, which
  cannot be changed in place, so `str()` and tracebacks show the title
  unchanged. The exceptions in the group, and a copy of the group rebuilt in
  another process from its redacted arguments, are redacted. Rebuilding the
  group in place of the original would break the in-place contract, because
  callers re-raise the original object. Messages that an exception builds in
  its own `__str__` method from other attributes are not covered either. The
  exceptions that the SDK, requests, urllib3, and web3's HTTP provider raise
  do not put URLs in such text. Code that creates an exception group must not
  put a URL in its title.
- **Local variables and object state are not redacted.** Tools that show
  local variables or object attributes, such as
  `traceback.TracebackException(capture_locals=True)`, debuggers, and error
  trackers, read process state directly. Configure scrubbing in those tools.
- **Only requests that the SDK sends are covered.** The SDK does not send
  JSON-RPC batch requests, which use the provider's `make_batch_request`
  method, so the provider does not redact them.
- **Node results are returned unchanged.** The provider redacts URLs
  everywhere in a response except its `result` member, which is node data.
  Object keys are redacted too. Two keys that reduce to the same text are
  merged, and the last value is kept.
- **Contract revert data is kept.** For a reverted call, `error.data` holds
  the contract's ABI-encoded error, which callers decode, for example to
  identify the contract's custom errors. It is hex, so the provider passes it
  through unchanged. web3 usually builds `ContractLogicError` from the
  node's error message, which is redacted. For data in the `Reverted 0x...`
  form, which some node clients return, web3 decodes the revert reason from
  the data after the provider returns, so the reason keeps its text. The
  contract sets the reason and does not see the node URL, so a reason
  contains a credential only if the caller passed one in the call data.
  Redacting the decoded reason would require changing the revert data, and
  the error's arguments would still carry the same text in hex.
- **Dependency loggers are not changed.** Dependency logs include request
  URLs: the debug logs of `web3.providers.HTTPProvider` and
  `urllib3.connectionpool`, and some urllib3 warnings, such as the warning
  for response headers that it cannot parse. Applications that enable these
  logs can add `vbase.utils.log.RedactUrlsFilter` to their handlers. For a
  record with exception information, the filter redacts the exception in
  place with `redact_exception`. The traceback that a formatter adds after
  the message is then redacted, and so is the application's exception
  object. The filter does not change stack information from
  `stack_info=True`, which shows source lines, not runtime values.
- **SQL database URLs are left to SQLAlchemy.** `SQLIndexingService` and the
  set matching services pass their `db_url` argument to SQLAlchemy, which
  hides the password, but not credentials in the query string, when it shows
  a database URL. The services do not log the URL.

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
