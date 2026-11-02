"""RFC 9457 Problem Details support for vBase HTTP APIs."""

import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, cast
from urllib.parse import urlsplit

import requests

PROBLEM_JSON_MEDIA_TYPE = "application/problem+json"
_STANDARD_MEMBERS = frozenset({"type", "title", "status", "detail", "instance"})
_PROBLEM_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


def _passes_uri_reference_checks(value: object) -> bool:
    """Apply lightweight sanity checks, not full RFC 3986 validation.

    Reject raw whitespace/control characters before urlsplit can discard them.
    URI references are identifiers here, not validated connection targets.
    """
    if (
        not isinstance(value, str)
        or not value.isascii()
        or any(ord(char) <= 32 or ord(char) == 127 for char in value)
        or _INVALID_PERCENT_ESCAPE.search(value)
    ):
        return False

    try:
        # A lowercase copy lets urlsplit recognize either IPvFuture marker case.
        # Do not use the parsed result to rewrite the original identifier.
        urlsplit(value.lower())
    except ValueError:
        return False
    return True


@dataclass(frozen=True)
class ProblemDetails:
    """A vBase Problem Details document with structural and basic URI checks."""

    type: str
    title: str
    status: int
    detail: str
    instance: Optional[str] = None
    extensions: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Check member structure and reject obviously malformed references."""
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
                _passes_uri_reference_checks(self.type),
                isinstance(self.title, str),
                status_valid,
                isinstance(self.detail, str),
                self.instance is None or _passes_uri_reference_checks(self.instance),
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
        """Parse a problem, or return ``None`` if SDK consumer checks fail."""
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
    """HTTP failure carrying a problem that passed SDK consumer checks.

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
        """Build an error after checking the response's media type and profile."""
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
