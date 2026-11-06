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

_LOG_FORMATTER = logging.Formatter(
    "[%(asctime)s][%(threadName)s][%(levelname)s]:%(message)s"
)


def _closing_text(match: re.Match) -> str:
    """Return the trailing punctuation of a matched URL that is surrounding text.

    Trailing punctuation can belong to a credential. It is surrounding text
    when the URL before it is already redacted, or from the closing quote or
    bracket for the character before the URL; the last such closer is used.
    """
    url = match[0]
    stripped = url.rstrip(_URL_TRAILING_PUNCTUATION)
    trailing = url[len(stripped) :]
    if _redact_url_value(stripped) == stripped:
        return trailing
    closer = _URL_CLOSERS.get(match.string[match.start() - 1 : match.start()])
    if closer is None or closer not in trailing:
        return ""
    return trailing[trailing.rindex(closer) :]


def _redact_url_value(url: str) -> str | None:
    """Keep a URL's scheme and host; redact credentials and the rest.

    :return: The redacted URL, or None if the value has no scheme.
    """
    parts = _URL_PARTS_PATTERN.fullmatch(url)
    if parts is None:
        return None
    host = parts["authority"].rpartition("@")[2]
    rest = parts["rest"]
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
    character before the URL, and after a URL that is already redacted.

    Text from ``://`` across whitespace to the first ``@`` that precedes any
    ``/``, ``?``, or ``#`` may be a URL with whitespace in its credentials. The
    whole span, including the text after the ``@``, is replaced with the scheme
    and ``[REDACTED]``. Use :func:`redact_url` for a value that is a single URL,
    such as a configured endpoint.

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
    :func:`redact_urls`, it removes credentials and replaces any path, query,
    or fragment with ``[REDACTED]``. A value without a scheme is replaced with
    ``[REDACTED]``.

    :param url: The complete URL value.
    :return: The redacted URL, or the original empty value.
    """
    if not url:
        return url
    redacted = _redact_url_value(url)
    if redacted is None:
        return REDACTED_LOG_VALUE
    return redacted


def redact_exception(error: BaseException) -> BaseException:
    """Apply :func:`redact_urls` to an exception's messages, in place.

    Errors from requests and urllib3 include the request URL in their
    messages, so a traceback can show credentials in a node URL. This redacts
    the string arguments of the exception, of exceptions in its arguments,
    such as the urllib3 error that a requests error wraps, and of its
    ``__cause__`` and ``__context__`` chain. The exception keeps its type and
    attributes, such as ``request`` and ``response``, so callers can still
    catch and inspect it. Redaction is idempotent.

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
        pending.extend(
            chained
            for chained in (current.__cause__, current.__context__)
            if chained is not None
        )
    return error


class RedactUrlsFilter(logging.Filter):  # pylint: disable=too-few-public-methods
    """Logging filter that applies :func:`redact_urls` to each formatted message.

    Attach it to loggers whose messages include provider or HTTP error text.
    """

    def filter(self, record: logging.LogRecord) -> bool:
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
