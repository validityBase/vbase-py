"""RFC 9457 Problem Details support for vBase HTTP APIs."""

import re
from dataclasses import dataclass, field
from ipaddress import IPv6Address
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, cast
from urllib.parse import urlsplit

import requests

PROBLEM_JSON_MEDIA_TYPE = "application/problem+json"
_STANDARD_MEMBERS = frozenset({"type", "title", "status", "detail", "instance"})
_URI_REFERENCE_CHARACTERS = re.compile(r"^[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]*$")
_INVALID_PERCENT_ENCODING = re.compile(r"%(?![0-9A-Fa-f]{2})")
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*$")
_URI_USER_INFO = re.compile(r"^[A-Za-z0-9\-._~!$&'()*+,;=:%]*$")
_URI_REG_NAME = re.compile(r"^[A-Za-z0-9\-._~!$&'()*+,;=%]*$")
_IPV_FUTURE = re.compile(r"^[vV][0-9A-Fa-f]+\.[A-Za-z0-9\-._~!$&'()*+,;=:]+$")
_PROBLEM_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")


def _is_valid_ip_literal(value: str) -> bool:
    """Return whether a bracketed host is an IPv6 address or IPvFuture."""
    if _IPV_FUTURE.fullmatch(value) is not None:
        return True
    if "%" in value:
        return False

    try:
        IPv6Address(value)
    except ValueError:
        return False
    return True


def _is_valid_authority(authority: str) -> bool:
    """Validate RFC 3986 authority syntax without DNS or TCP port semantics.

    ``urlsplit`` splits authority but does not validate it. Its ``port``
    property also rejects digit-only ports above 65535, which RFC 3986 allows.
    """
    if authority.count("@") > 1:
        return False

    user_info, separator, host_port = authority.rpartition("@")
    if separator and _URI_USER_INFO.fullmatch(user_info) is None:
        return False

    if host_port.startswith("["):
        closing_bracket = host_port.find("]")
        host = host_port[1:closing_bracket]
        port_separator = host_port[closing_bracket + 1 :]
        return (
            closing_bracket >= 0
            and _is_valid_ip_literal(host)
            and (
                port_separator in ("", ":")
                or port_separator.startswith(":")
                and port_separator[1:].isdigit()
            )
        )

    if host_port.count(":") > 1:
        return False
    host, port_separator, port = host_port.rpartition(":")
    if not port_separator:
        host = port
        port = ""
    if _URI_REG_NAME.fullmatch(host) is None:
        return False
    return not port_separator or not port or port.isdigit()


def _is_uri_reference(value: object) -> bool:
    """Return whether a string is a well-formed RFC 3986 URI reference.

    ``urllib.parse`` intentionally accepts malformed input, so the split is
    combined with lightweight checks for the RFC 3986 character set, percent
    encoding, fragments, schemes, and relative-path syntax.
    """
    if (
        not isinstance(value, str)
        or _URI_REFERENCE_CHARACTERS.fullmatch(value) is None
        or _INVALID_PERCENT_ENCODING.search(value)
        or value.count("#") > 1
    ):
        return False

    try:
        parsed = urlsplit(value)
    except ValueError:
        return False

    invalid_scheme = bool(
        parsed.scheme and _URI_SCHEME.fullmatch(parsed.scheme) is None
    )
    brackets_outside_authority = any(
        "[" in component or "]" in component
        for component in (parsed.path, parsed.query, parsed.fragment)
    )
    invalid_authority = bool(parsed.netloc and not _is_valid_authority(parsed.netloc))

    # RFC 3986 path-noscheme forbids a colon in the first relative segment.
    first_path_segment = parsed.path.split("/", maxsplit=1)[0]
    invalid_relative_path = bool(
        not parsed.scheme and not value.startswith("//") and ":" in first_path_segment
    )

    return not any(
        (
            invalid_scheme,
            brackets_outside_authority,
            invalid_authority,
            invalid_relative_path,
        )
    )


@dataclass(frozen=True)
class ProblemDetails:
    """A validated vBase RFC 9457 Problem Details document."""

    type: str
    title: str
    status: int
    detail: str
    instance: Optional[str] = None
    extensions: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Reject invalid documents created through the public constructor."""
        extensions_valid = isinstance(self.extensions, Mapping) and not any(
            member in self.extensions for member in _STANDARD_MEMBERS
        )
        code = self.extensions.get("code") if extensions_valid else None
        details = self.extensions.get("details") if extensions_valid else None
        status_valid = (
            isinstance(self.status, int)
            and not isinstance(self.status, bool)
            and 100 <= self.status <= 599
        )
        details_valid = bool(
            extensions_valid
            and ("details" not in self.extensions or isinstance(details, dict))
        )
        code_valid = bool(
            isinstance(code, str) and _PROBLEM_CODE.fullmatch(code) is not None
        )
        if not all(
            (
                _is_uri_reference(self.type),
                isinstance(self.title, str),
                status_valid,
                isinstance(self.detail, str),
                self.instance is None or _is_uri_reference(self.instance),
                code_valid,
                details_valid,
            )
        ):
            raise ValueError("Invalid vBase Problem Details document")
        object.__setattr__(
            self,
            "extensions",
            MappingProxyType(dict(self.extensions)),
        )

    @classmethod
    def from_dict(cls, payload: object) -> Optional["ProblemDetails"]:
        """Parse the vBase Problem Details schema, or return ``None``."""
        if not isinstance(payload, dict) or (
            "instance" in payload and payload["instance"] is None
        ):
            return None

        try:
            return cls(
                type=payload.get("type"),
                title=payload.get("title"),
                status=payload.get("status"),
                detail=payload.get("detail"),
                instance=payload.get("instance"),
                extensions={
                    key: value
                    for key, value in payload.items()
                    if key not in _STANDARD_MEMBERS
                },
            )
        except ValueError:
            return None

    @property
    def code(self) -> str:
        """Return the required vBase machine-readable code extension."""
        return cast(str, self.extensions["code"])

    @property
    def details(self) -> Optional[Dict[str, Any]]:
        """Return the optional vBase diagnostic details extension."""
        details = self.extensions.get("details")
        return details if isinstance(details, dict) else None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the problem document."""
        extensions = {
            key: value
            for key, value in self.extensions.items()
            if key not in _STANDARD_MEMBERS
        }
        return {
            "type": self.type,
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
            **({"instance": self.instance} if self.instance is not None else {}),
            **extensions,
        }


class ProblemDetailsError(requests.HTTPError):
    """HTTP failure carrying a validated RFC 9457 problem document.

    The exception remains compatible with callers that catch
    :class:`requests.HTTPError` and preserves the original response.
    """

    def __init__(
        self,
        response: requests.Response,
        problem: ProblemDetails,
    ) -> None:
        self.problem = problem

        error_text = (
            f"HTTP API problem ({problem.status}) [{problem.type}] "
            f"code={problem.code}: {problem.detail}"
        )
        super().__init__(
            error_text,
            response=response,
            request=response.request,
        )

    @property
    def type(self) -> str:
        """Return the problem type URI reference."""
        return self.problem.type

    @property
    def title(self) -> str:
        """Return the short problem title."""
        return self.problem.title

    @property
    def status(self) -> int:
        """Return the HTTP status from the problem document."""
        return self.problem.status

    @property
    def detail(self) -> str:
        """Return the occurrence-specific explanation."""
        return self.problem.detail

    @property
    def instance(self) -> Optional[str]:
        """Return the optional occurrence correlation URI reference."""
        return self.problem.instance

    @property
    def extensions(self) -> Mapping[str, Any]:
        """Return all Problem Details extension members."""
        return self.problem.extensions

    @property
    def code(self) -> str:
        """Return the required vBase code extension."""
        return self.problem.code

    @property
    def details(self) -> Optional[Dict[str, Any]]:
        """Return the optional vBase details extension."""
        return self.problem.details

    @classmethod
    def from_response(
        cls, response: requests.Response
    ) -> Optional["ProblemDetailsError"]:
        """Build an error from a conforming RFC 9457 HTTP response."""
        content_type = response.headers.get("Content-Type", "")
        media_type = content_type.partition(";")[0].strip().lower()
        if media_type != PROBLEM_JSON_MEDIA_TYPE:
            return None

        try:
            payload = response.json()
        except ValueError:
            return None

        problem = ProblemDetails.from_dict(payload)
        if problem is None or problem.status != response.status_code:
            return None

        return cls(response, problem)
