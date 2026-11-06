---
applyTo: "vbase/**/*.py"
---

# URL redaction review notes

The SDK reduces URLs to their scheme and host in its log messages and in the
errors that node and forwarder requests raise. The code is in
`vbase/utils/log.py` and in `_RedactingHTTPProvider` in
`vbase/core/web3_http_commitment_service.py`.

The limits below are by design. Do not report them as defects. Report a
finding only when a change affects one of these limits, or when it shows a path
that the SDK, requests, urllib3, or web3 takes that leaves a credential in a
message, traceback, or log.

- Free-text URL matching is best effort. Unencoded credentials that combine
  whitespace with `@`, `/`, `?`, or `#` may be only partly redacted. When a
  quote or bracket comes before a URL, the punctuation from the matching
  closing character onward is kept.
- Attributes that callers use keep their values: `request` and `response` of
  requests errors, `ProblemDetailsError` attributes, `JSONDecodeError.doc`,
  and the provider's `endpoint_uri`.
- An exception group's title is its read-only `message` attribute, so
  `str()` and tracebacks show it unchanged. The exceptions in the group are
  redacted. Rebuilding the group would break the in-place contract of
  `redact_exception`, whose callers re-raise the original object.
- Messages that an exception builds in its own `__str__` method from
  attributes other than the redacted ones are not covered. The exceptions
  that the SDK, requests, urllib3, and web3's HTTP provider raise do not put
  URLs in such text.
- Local variables and object state that debuggers, `capture_locals`, and
  error trackers show are not redacted.
- JSON-RPC batch requests are not redacted; the SDK does not send them.
- The `result` member of a node response is returned unchanged. URLs in other
  members, including object keys, are redacted, and keys that reduce to the
  same text are merged.
- The SDK does not change dependency loggers. `RedactUrlsFilter` redacts a
  logged exception in place and does not change `stack_info`.
- The SQL services pass `db_url` to SQLAlchemy, which hides the password but
  not query-string credentials when it shows the URL. The services do not log
  it.

The rationale for each limit is in the "URL Redaction Limits" section of
`internal/specs/development.md`. Keep this list in sync with that section.
