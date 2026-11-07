"""Default logging interface"""

import logging
import re

REDACTED_LOG_VALUE = "[REDACTED]"
_API_KEY_VISIBLE_CHARS = 8
_API_KEY_MIN_HIDDEN_CHARS = 16

# URLs and paths run to the next whitespace. Credentials can contain
# apostrophes, quotes, and other delimiters that requests accepts, so ending a
# match at one of them could leave the rest of a credential in the text.
# An absolute URL, such as scheme://user:password@host:port/path?query#fragment.
# Server-supplied text can also contain whitespace in a URL's userinfo, so a
# match may run across whitespace to the first "@" that precedes any "/", "?",
# or "#". Coverage is limited to expected credential forms; see redact_urls.
# Schemes are matched up to 64 characters, which covers registered schemes.
# Without a limit, long dotted or hyphenated text makes matching quadratic.
_URL_PATTERN = re.compile(
    r"\b[a-z][a-z0-9+.-]{0,63}://(?:[^/?#@]*@)?\S*", re.IGNORECASE
)
_URL_PARTS_PATTERN = re.compile(
    r"(?P<scheme>[^:]+://)(?P<authority>[^/?#]*)(?P<rest>.*)", re.DOTALL
)
# A request path without a host, as in urllib3's "Max retries exceeded with url: /path".
_URL_PATH_PATTERN = re.compile(r"(?P<prefix>\burl: )(?P<path>/\S+)")
# Punctuation and closing quotes that can follow a URL in error text.
# "]" is excluded so that redacting a redacted URL changes nothing.
_URL_TRAILING_PUNCTUATION = ".,:;!?)}>'\""
# Opening characters before a URL, mapped to the characters that close them.
_URL_CLOSERS = {"'": "'", '"': '"', "(": ")", "<": ">", "{": "}"}
# Text that can follow a port when a URL is in brackets or other text.
_PORT_TRAILING_TEXT = _URL_TRAILING_PUNCTUATION + "]"
_MAX_PORT = 65535

_LOG_FORMATTER = logging.Formatter(
    "[%(asctime)s][%(threadName)s][%(levelname)s]:%(message)s"
)


def _closing_text(match: re.Match) -> str:
    """Return the trailing punctuation of a matched URL that is surrounding text.

    Trailing punctuation can belong to a credential. It is surrounding text
    when it follows a redacted path, or from the closing quote or bracket for
    the character before the URL; the last such closer is used.
    """
    url = match[0]
    stripped = url.rstrip(_URL_TRAILING_PUNCTUATION)
    trailing = url[len(stripped) :]
    # Only a redacted path is already redacted. After a bare host or "/", the
    # punctuation is the whole path or query, such as "/!!!" or "?!!!".
    if (
        stripped.endswith("/" + REDACTED_LOG_VALUE)
        and _redact_url_value(stripped) == stripped
    ):
        return trailing
    closer = _URL_CLOSERS.get(match.string[match.start() - 1 : match.start()])
    if closer is None or closer not in trailing:
        return ""
    return trailing[trailing.rindex(closer) :]


def _has_invalid_port(host: str) -> bool:
    """Return whether a URL's host part has a port that is not a valid number.

    The host part is a host name, IPv4 address, or bracketed IPv6 address,
    optionally followed by ":" and a port from 0 to 65535. A host name or IPv4
    address cannot contain ":", so the port is all the text after the first
    ":". Punctuation that can follow a URL in text, and "]", are ignored after
    the port's digits, but a port without digits, such as ":!!!", is invalid.

    A URL with credentials but no host, such as ``https://user:1234``, has the
    form of a host and port, so it is not detected. See "URL Redaction Limits"
    in ``internal/specs/development.md``.
    """
    if host.startswith("["):
        _, bracket, after = host.partition("]")
        if not bracket:
            return True
        if not after.startswith(":"):
            return bool(after.rstrip(_PORT_TRAILING_TEXT))
        port = after[1:]
    else:
        _, colon, port = host.partition(":")
        if not colon:
            return False
    if not port:
        return False
    digits = port.rstrip(_PORT_TRAILING_TEXT)
    return not (digits.isascii() and digits.isdigit() and int(digits) <= _MAX_PORT)


def _redact_url_value(url: str) -> str | None:
    """Keep a URL's scheme and host; redact credentials and the rest.

    In a malformed URL, the text in the host position can be credentials. An
    "@" after the host part means that the credentials contain an unencoded
    "/", "?", or "#", which ended the host part early. A port that is not a
    number from 0 to 65535 can be a password whose "@host" is missing. For
    such a URL, only the scheme is kept.

    :return: The redacted URL, or None if the value has no scheme.
    """
    parts = _URL_PARTS_PATTERN.fullmatch(url)
    if parts is None:
        return None
    host = parts["authority"].rpartition("@")[2]
    rest = parts["rest"]
    if "@" in rest or _has_invalid_port(host):
        return parts["scheme"] + REDACTED_LOG_VALUE
    if rest not in ("", "/"):
        rest = "/" + REDACTED_LOG_VALUE
    return f"{parts['scheme']}{host}{rest}"


def _redact_url_match(match: re.Match) -> str:
    """Redact a URL found in text."""
    url = match[0]
    if any(char.isspace() for char in url):
        # Whitespace occurs only before an "@". The match is either a URL with
        # whitespace in its credentials or a URL followed by unrelated text, so
        # the text after the "@" is not a trusted host. Keep only the scheme.
        redacted = f"{url.partition('://')[0]}://{REDACTED_LOG_VALUE}"
    else:
        redacted = _redact_url_value(url) or url
    if redacted.endswith(REDACTED_LOG_VALUE):
        # Trailing punctuation was redacted with the URL.
        redacted += _closing_text(match)
    return redacted


def _redact_url_path_match(match: re.Match) -> str:
    """Redact a request path reported without its host.

    Trailing punctuation is redacted with the path, because it can belong to
    a credential.
    """
    if match["path"] == "/":
        return match[0]
    return f"{match['prefix']}/{REDACTED_LOG_VALUE}"


def redact_urls(text: object) -> str:
    """Reduce URLs in text to scheme and host for safe diagnostics.

    RPC providers commonly embed credentials in a URL's userinfo, path, or
    query. This removes userinfo, replaces any path, query, or fragment with
    ``[REDACTED]``, and does the same for bare request paths reported by
    urllib3. Hosts and ports are kept to identify the failing endpoint.
    A URL extends to the next whitespace, so text attached to it without a
    space is redacted with it. Punctuation at the end of a URL with a path,
    query, or fragment is redacted with them, because it can belong to a
    credential. It is kept from the closing quote or bracket for the
    character before the URL, and after a path that is already redacted.

    Text from ``://`` across whitespace to the first ``@`` that precedes any
    ``/``, ``?``, or ``#`` may be a URL with whitespace in its credentials. The
    whole span, including the text after the ``@``, is replaced with the scheme
    and ``[REDACTED]``. Use :func:`redact_url` for a value that is a single URL,
    such as a configured endpoint.

    A malformed URL whose host part may hold credentials is also replaced with
    its scheme and ``[REDACTED]``. This applies when an ``@`` follows the host
    part, because an unencoded ``/``, ``?``, or ``#`` in the credentials ended
    it early. It also applies when the port is not a number from 0 to 65535,
    because it may be a password whose ``@host`` is missing.

    Redaction is best effort. It covers the credential forms expected in SDK
    messages and in error text from requests, urllib3, web3, and the vBase
    forwarder. Unencoded credentials that combine whitespace with ``@``,
    ``/``, ``?``, or ``#`` are not expected and may be only partly redacted.
    If such a case is identified, handle it separately.

    :param text: The text, or an object such as an exception, to redact.
    :return: The redacted text.
    """
    redacted = _URL_PATTERN.sub(_redact_url_match, str(text))
    return _URL_PATH_PATTERN.sub(_redact_url_path_match, redacted)


def redact_url(url: str | None) -> str | None:
    """Reduce one complete URL value, such as a configured endpoint, to its
    scheme and host.

    :func:`redact_urls` finds URLs in free text, where a URL ends at
    whitespace. Requests accepts whitespace in a configured URL's credentials,
    so use this function when the whole value is a single URL. Like
    :func:`redact_urls`, it removes credentials, replaces any path, query, or
    fragment with ``[REDACTED]``, and keeps only the scheme of a malformed URL
    whose host part may hold credentials. A value without a scheme is
    replaced with ``[REDACTED]``.

    :param url: The complete URL value.
    :return: The redacted URL, or the original empty value.
    """
    if not url:
        return url
    redacted = _redact_url_value(url)
    if redacted is None:
        return REDACTED_LOG_VALUE
    return redacted


def _redact_request_target(target: str) -> str:
    """Redact a request URL, or a request path reported without its host."""
    if "://" in target:
        return redact_url(target)
    if target.startswith("/") and target != "/":
        return "/" + REDACTED_LOG_VALUE
    return target


def _redact_exception_attributes(error: BaseException) -> None:
    """Redact the attributes, other than args, that :func:`redact_exception` covers."""
    if isinstance(error, OSError):
        # An OSError message is built from these attributes, not from args.
        for name in ("strerror", "filename", "filename2"):
            value = getattr(error, name)
            if isinstance(value, str):
                setattr(error, name, redact_urls(value))
    # Tracebacks show notes after the message. urllib3 errors and
    # JSONDecodeError rebuild their message from the other attributes when
    # passed to another process (see redact_exception). Write to the instance
    # dictionary, because other classes may define these names as read-only
    # properties.
    attributes = getattr(error, "__dict__", {})
    notes = attributes.get("__notes__")
    if isinstance(notes, list):
        attributes["__notes__"] = [
            redact_urls(note) if isinstance(note, str) else note for note in notes
        ]
    if isinstance(attributes.get("url"), str):
        attributes["url"] = _redact_request_target(attributes["url"])
    for name in ("_message", "msg"):
        if isinstance(attributes.get(name), str):
            attributes[name] = redact_urls(attributes[name])


def redact_exception(error: BaseException) -> BaseException:
    """Apply :func:`redact_urls` to an exception's messages, in place.

    Errors from requests and urllib3 include the request URL in their
    messages, so a traceback can show credentials in a node URL. This redacts
    the string arguments of the exception, of exceptions in its arguments,
    such as the urllib3 error that a requests error wraps, of the exceptions
    in an exception group, and of its ``__cause__`` and ``__context__``
    chain. For each of these exceptions, it also redacts the other text that
    a traceback shows: the exception's notes, and the ``strerror``,
    ``filename``, and ``filename2`` attributes of an :class:`OSError`, whose
    message is built from these attributes instead of its arguments.

    It also redacts the attributes that some exceptions are rebuilt from in
    another process. An exception that is passed to another process, for
    example from a ``concurrent.futures.ProcessPoolExecutor`` or
    ``multiprocessing`` worker to its parent, is serialized with
    :mod:`pickle` and rebuilt in the receiving process. Most exceptions are
    rebuilt from their arguments, which are already redacted. urllib3 errors
    are rebuilt from their ``url`` and ``_message`` attributes instead, and
    :class:`json.JSONDecodeError`, which web3 raises with the text of a node
    response that is not JSON, from its ``msg`` attribute. Leaving these
    attributes unredacted would put the original text back into the rebuilt
    message.

    The exception keeps its type and other attributes, such as ``request``
    and ``response``, so callers can still catch and inspect it. Redaction is
    idempotent.

    By design, some text is not redacted. The title of an exception group is
    its read-only ``message`` attribute, which cannot be changed in place, so
    ``str()`` and tracebacks show the title unchanged; the exceptions in the
    group are redacted. Messages that an exception builds in its own
    ``__str__`` method from other attributes are not covered either. The
    exceptions that the SDK, requests, urllib3, and web3's HTTP provider raise
    do not put URLs in such text. The full list of limits is in the "URL
    Redaction Limits" section of ``internal/specs/development.md`` in the
    repository.

    :param error: The exception to redact.
    :return: The same exception.
    """
    pending = [error]
    seen = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        args = []
        changed = False
        for arg in current.args:
            if isinstance(arg, BaseException):
                pending.append(arg)
            elif isinstance(arg, str):
                redacted = redact_urls(arg)
                changed = changed or redacted != arg
                arg = redacted
            args.append(arg)
        if changed:
            current.args = tuple(args)
        _redact_exception_attributes(current)
        if isinstance(current, BaseExceptionGroup):
            # By design, the group's title keeps its text: it is the read-only
            # message attribute (see the docstring). Its exceptions are redacted.
            pending.extend(current.exceptions)
        pending.extend(
            chained
            for chained in (current.__cause__, current.__context__)
            if chained is not None
        )
    return error


class RedactUrlsFilter(logging.Filter):  # pylint: disable=too-few-public-methods
    """Logging filter that applies :func:`redact_urls` to each formatted message.

    Attach it to loggers whose messages include provider or HTTP error text.
    Formatters add the traceback of a record's exception after the message,
    so the filter also redacts that exception in place with
    :func:`redact_exception`, and redacts traceback text that a handler
    formatted before the filter ran. Stack information from
    ``stack_info=True`` shows source lines, not runtime values, and is not
    changed.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.exc_info and record.exc_info[1] is not None:
            redact_exception(record.exc_info[1])
            # Clear traceback text that a handler cached before this filter
            # ran, so that the redacted exception is formatted again.
            record.exc_text = None
        elif record.exc_text:
            record.exc_text = redact_urls(record.exc_text)
        try:
            message = record.getMessage()
        except (TypeError, ValueError):
            # Leave malformed records for the handler's standard error reporting.
            return True
        record.msg = redact_urls(message)
        record.args = None
        return True


def mask_api_key(api_key: str | None) -> str | None:
    """Mask an API key for safe diagnostic logging.

    Keys with at least sixteen hidden characters retain eight characters at
    each end for correlation. Shorter keys are fully redacted.

    :param api_key: The API key to mask.
    :return: The masked API key, or the original empty value.
    """
    if not api_key:
        return api_key
    if len(api_key) < _API_KEY_VISIBLE_CHARS * 2 + _API_KEY_MIN_HIDDEN_CHARS:
        return REDACTED_LOG_VALUE
    return (
        f"{api_key[:_API_KEY_VISIBLE_CHARS]}"
        f"\N{HORIZONTAL ELLIPSIS}"
        f"{api_key[-_API_KEY_VISIBLE_CHARS:]}"
    )


def get_default_logger(name: str) -> logging.Logger:
    """Get default logger for a given name.

    :param name: The logger name.
    :return: The logger object.
    """
    # Set Null log handler to avoid "No handlers could be found for logger XXX".
    # This is important for library code, which may contain code to log events
    # if a user of the library does not configure logging.
    if len(logging.getLogger().handlers) == 0:
        logging.getLogger().addHandler(logging.NullHandler())

    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    # Add a handler for the log if one isn't present.
    if len(log.handlers) == 0:
        handler = logging.StreamHandler()
        handler.setFormatter(_LOG_FORMATTER)
        log.addHandler(handler)
        log.propagate = False

    return log
