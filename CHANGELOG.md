# Changelog

All notable changes are documented here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Earlier releases were not tracked in this file; see the
[commit history](https://github.com/validityBase/vbase-py/commits/main/)
for prior changes.

## [Unreleased]

### Added

- Raise `ProblemDetailsError` for Forwarder HTTP errors that return a valid
  RFC 9457 `application/problem+json` body. It subclasses
  `requests.HTTPError`, keeps the original response, and exposes the problem's
  `type`, `title`, `status`, `detail`, `instance`, `code`, `details`, and
  `extensions`. `ProblemDetails` and `ProblemDetailsError` are exported from
  `vbase` and can be pickled. See
  [Forwarder errors](https://github.com/validityBase/vbase-py/blob/main/docs/errors.rst)
  (#99, #134).
- Add an optional `node_rpc_url` argument to
  `IndexingService.create_instance_from_commitment_service()`. Use it to query
  events through your own node instead of the node URL returned by the
  forwarder. It applies only to forwarder commitment services; an empty value,
  or any value with a Web3 commitment service, raises `ValueError` (#135).
- Document endpoint selection and event-query requirements in
  [Indexing and Node Requirements](https://github.com/validityBase/vbase-py/blob/main/README.md#indexing-and-node-requirements)
  (#135).

### Changed

- For Forwarder errors raised as `ProblemDetailsError`, `str(error)` and
  `error.args` contain `HTTP API problem (<status>) code=<code>: <detail>`
  instead of the `requests` message
  `<status> Client Error: <reason> for url: <url>`. Handlers that catch
  `requests.HTTPError` and code that reads `error.response` work as before.
  Code that parses the message should use the error's attributes (#99).
- `Web3HTTPIndexingService` queries events with stateless `eth_getLogs`
  requests instead of `eth_newFilter` and `eth_getFilterLogs`, so queries work
  with load-balanced endpoints. A query that fails with a connection error or
  a JSON-RPC error from the node is retried in full. Other errors, such as
  invalid query arguments, are raised without a retry. Previously, only log
  retrieval was retried, for any error.
- `IndexingService.create_instance_from_commitment_service()` passes only
  `node_rpc_url` and `commitment_service_address` from the forwarder's
  commitment service data to `Web3HTTPCommitmentService` and ignores other
  fields. It raises `ValueError` if the forwarder returns an empty or null
  `node_rpc_url`; previously, Web3 used its default local endpoint.
- Forwarder and failover indexing error logs use these formats:
  `HTTP error occurred: api=<api> status=<status> error=<error>`,
  `Request error occurred: api=<api> error=<error>`, and
  `Service <class> failed to execute <method> with error: <type>: <error>`.
- Declare Python 3.11 through 3.13 in the package classifiers, and verify
  installation on Python 3.13 in CI (#100).

### Fixed

- Read `VBASE_COMMITMENT_SERVICE_INJECT_GETH_POA_MIDDLEWARE` in
  `Web3HTTPCommitmentService.create_instance_from_env()`. The setting was
  ignored, so the POA middleware was never injected. Deployments that set it
  to a true value now get the middleware.
- Clamp event-query start blocks at genesis when `n_last_blocks` exceeds the
  chain height (#135).
- When `find_objects()`, `find_object()`, `find_last_object()`, or
  `find_user_objects()` return set CIDs, use the same start block for the
  object and set queries. Previously, `setCid` could be missing for
  commitments near the start of the `n_last_blocks` range.
- When all services fail, `FailoverIndexingService` raises its `RuntimeError`
  with the last service error as the cause.

### Security

- Keep credentials out of SDK logs and exception messages (#96):
  - `get_init_args_from_env()` debug logs show whether the forwarder URL, node
    URL, and private key are set, and a masked API key, instead of the values.
  - Indexing descriptor logs show only the number of commitment services.
  - Forwarder, node connection, indexing retry, and failover logs keep the
    error text but reduce each URL to its scheme and host. URL credentials,
    paths, queries, and fragments, and punctuation that directly follows
    them, are replaced with `[REDACTED]`. A URL that may have whitespace in
    its credentials keeps only its scheme.
  - `Web3HTTPCommitmentService` connection errors show only the scheme and
    host of the node URL, including when its credentials contain whitespace.
    A node URL without a scheme is shown as `[REDACTED]`.
  - `ForwarderCommitmentService` raises `ValueError` when it is created with
    a `forwarder_url` that is not an `http` or `https` URL with a host, or
    that has whitespace or control characters after any leading whitespace.
    For such a URL, `requests` fails with the URL in its error messages in a
    form that URL redaction cannot recognize: without a scheme, with escapes
    such as `https:\t//`, or split by whitespace. The `ValueError` message
    does not include the URL. The forwarder authenticates with the
    `X-API-KEY` header, so its URL needs no credentials or whitespace.
  - Errors raised by node and forwarder requests reduce URLs in their
    messages and notes to scheme and host. These include `requests` HTTP and
    connection errors, the `urllib3` errors they wrap, and the errors that
    web3 raises for JSON-RPC error responses from the node, such as
    `Web3RPCError`. This applies to tracebacks, to the cause of the
    `FailoverIndexingService` `RuntimeError`, and to errors passed to another
    process, for example from a `ProcessPoolExecutor` worker to its parent.
  - These errors keep their type and attributes, except attributes that hold
    the same text, which are redacted too: `Web3RPCError.rpc_response`, the
    `strerror` and file names of an `OSError`, and the `url` attribute of
    `urllib3` errors and the `msg` attribute of `JSONDecodeError`, from which
    these errors rebuild their messages in the receiving process.
    `error.request.url` keeps the full URL.
  - `ProblemDetailsError` messages omit the server-supplied `type` URI and
    redact URLs in `detail`.

## [2.0.0] - 2026-10-28

### Changed

- Require Python 3.11 or later. Drop support for Python 3.8 through 3.10 (#84).
- Accept Web3 versions greater than or equal to 7.15 and less than 9 (#106).
- Set upper bounds for runtime dependencies (#63, #106).
- Remove the unused `imageio` runtime dependency (#110).

### Fixed

- Use a matching `AddSet` receipt event to confirm a new set commitment.
  Check the event's user and set CID. This avoids a false `add_set` failure
  when an RPC backend returns stale state (#86).
- Retry the state check if a successful `add_set` transaction has no `AddSet`
  receipt event. Fail if the set commitment cannot be confirmed after the
  configured retries. Commitment confirmation does not guarantee immediate
  visibility through an indexing service (#86).
