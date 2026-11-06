Forwarder errors
================

HTTP failures whose response uses the ``application/problem+json`` media type
and passes the SDK's vBase `RFC 9457 <https://www.rfc-editor.org/rfc/rfc9457>`_
consumer checks raise
``ProblemDetailsError``. It remains a subclass of ``requests.HTTPError`` and
preserves the original response. The standard members are exposed as ``type``,
``title``, ``status``, ``detail``, and ``instance``; additional members are in
``extensions``. The required vBase ``code`` and optional ``details`` extensions
have convenience properties. Clients should branch primarily on ``type``.

The exception message, ``str(error)`` and ``error.args``, has the form
``HTTP API problem (<status>) code=<code>: <detail>``. It is not the
``"<status> Client Error: <reason> for url: <url>"`` message that
``requests.HTTPError`` uses for other failures. The message omits the
server-supplied ``type`` URI and reduces any URL in ``detail`` to its scheme
and host, or to its scheme alone when the URL may have whitespace in its
credentials, so it does not copy common forms of URL credentials into logs or
tracebacks. This redaction is best effort; API producers must not include
credentials in ``detail``. The
attributes keep the original values. The message is for people to read and may
change; do not parse it. To branch on a failure, use ``type``, ``code``,
``status``, or ``error.response.status_code``.

The parsed document is available as ``error.problem`` and can be serialized
with ``error.problem.to_dict()``. Both the parsed document and the
exception support Python pickling, including passing structured errors
between worker processes.

API producers must supply RFC 3986 URI references in the required ``type`` and
optional ``instance`` members; ``instance`` may be omitted, but not JSON null.
The SDK deliberately applies lightweight URI checks, not complete RFC 3986
validation: values must be ASCII strings without raw whitespace or control
characters, have well-formed percent escapes, and parse with
``urllib.parse.urlsplit``. It does not implement its own authority, IP, or port
validation. Passing these checks does not certify RFC conformance or URL safety.
Identifiers retain their original case and escaping and are not dereferenced.

The optional vBase ``details`` extension may also be omitted, but when present
it must be a JSON object.
The required ``code`` extension must be an uppercase machine-readable identifier
made up of ASCII letters, digits, and underscores, with at most 64 characters.
Responses that fail these SDK checks, contain invalid required vBase
members, use non-Problem-Details content, or have a status mismatch continue to
raise the original ``requests.HTTPError`` instead of being interpreted as a
structured error.

The `vBase SDK Problem Details consumer profile
<https://github.com/validityBase/vbase-py/blob/main/docs/problem-details.schema.json>`_
documents the forward-compatible response structure. Its ``uri-reference``
format annotations describe the producer contract, not an exhaustive SDK
format-validation guarantee. It
intentionally does not enumerate producer-specific error codes or detail fields;
each API owns that authoritative contract.
