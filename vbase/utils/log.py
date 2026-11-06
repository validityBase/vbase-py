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
_URL_PATTERN = re.compile(r"\b[a-z][a-z0-9+.-]*://\S+", re.IGNORECASE)
_URL_PARTS_PATTERN = re.compile(
    r"(?P<scheme>[^:]+://)(?P<authority>[^/?#]*)(?P<rest>.*)", re.DOTALL
)
# A request path without a host, as in urllib3's "Max retries exceeded with url: /path".
_URL_PATH_PATTERN = re.compile(r"(?P<prefix>\burl: )(?P<path>/\S+)")
# Punctuation and closing quotes that follow a URL in error text rather than
# ending it. "]" is excluded so that redacting a redacted URL changes nothing.
_URL_TRAILING_PUNCTUATION = ".,:;!?)}>'\""

_LOG_FORMATTER = logging.Formatter(
    "[%(asctime)s][%(threadName)s][%(levelname)s]:%(message)s"
)


def _split_trailing_punctuation(text: str) -> tuple[str, str]:
    """Split text into a URL or path and the punctuation that follows it."""
    stripped = text.rstrip(_URL_TRAILING_PUNCTUATION)
    return stripped, text[len(stripped) :]


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
    """Redact a URL found in text, keeping the punctuation that follows it."""
    url, suffix = _split_trailing_punctuation(match[0])
    redacted = _redact_url_value(url)
    if redacted is None:
        return match[0]
    return redacted + suffix


def _redact_url_path_match(match: re.Match) -> str:
    """Redact a request path reported without its host."""
    path, suffix = _split_trailing_punctuation(match["path"])
    if path == "/":
        return match[0]
    return f"{match['prefix']}/{REDACTED_LOG_VALUE}{suffix}"


def redact_urls(text: object) -> str:
    """Reduce URLs in text to scheme and host for safe diagnostics.

    RPC providers commonly embed credentials in a URL's userinfo, path, or
    query. This removes userinfo, replaces any path, query, or fragment with
    ``[REDACTED]``, and does the same for bare request paths reported by
    urllib3. Hosts and ports are kept to identify the failing endpoint.
    A URL extends to the next whitespace, so text attached to it without a
    space is redacted with it. Use :func:`redact_url` for a value that is a
    single URL, such as a configured endpoint.

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
