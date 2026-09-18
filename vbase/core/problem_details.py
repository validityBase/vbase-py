"""RFC 9457 Problem Details support for vBase HTTP APIs."""

import json
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Optional

import requests
from rfc3986_validator import validate_rfc3986

PROBLEM_JSON_MEDIA_TYPE = "application/problem+json"
_STANDARD_MEMBERS = frozenset({"type", "title", "status", "detail", "instance"})


def _is_uri_reference(value: object) -> bool:
    """Return whether a value follows the RFC 3986 URI-reference grammar."""
    return isinstance(value, str) and bool(
        validate_rfc3986(value, rule="URI_reference")
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

    @classmethod
    def from_dict(cls, payload: object) -> Optional["ProblemDetails"]:
        """Parse the final vBase Problem Details schema, or return ``None``."""
        if not isinstance(payload, dict):
            return None

        problem_type = payload.get("type")
        title = payload.get("title")
        status = payload.get("status")
        detail = payload.get("detail")
        instance = payload.get("instance")
        code = payload.get("code")
        details = payload.get("details")
        problem_type_valid = _is_uri_reference(problem_type)
        required_strings_valid = all(
            isinstance(value, str) for value in (title, detail)
        )
        status_valid = (
            isinstance(status, int)
            and not isinstance(status, bool)
            and 100 <= status <= 599
        )
        code_valid = isinstance(code, str) and bool(code)
        instance_valid = "instance" not in payload or _is_uri_reference(instance)
        details_valid = "details" not in payload or isinstance(details, dict)
        if not all(
            (
                problem_type_valid,
                required_strings_valid,
                status_valid,
                code_valid,
                instance_valid,
                details_valid,
            )
        ):
            return None

        return cls(
            type=problem_type,
            title=title,
            status=status,
            detail=detail,
            instance=instance,
            extensions={
                key: value
                for key, value in payload.items()
                if key not in _STANDARD_MEMBERS
            },
        )

    @property
    def code(self) -> Optional[str]:
        """Return the optional vBase machine-readable code extension."""
        code = self.extensions.get("code")
        return code if isinstance(code, str) else None

    @property
    def details(self) -> Optional[dict[str, Any]]:
        """Return the optional vBase diagnostic details extension."""
        details = self.extensions.get("details")
        return details if isinstance(details, dict) else None

    def with_extensions(self, **extensions: Any) -> "ProblemDetails":
        """Return a copy with additional non-standard members."""
        safe_extensions = {
            key: value
            for key, value in extensions.items()
            if key not in _STANDARD_MEMBERS
        }
        return replace(
            self,
            extensions={**self.extensions, **safe_extensions},
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the validated problem document."""
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
            f"HTTP API problem ({problem.status}) [{problem.type}]: "
            f"{problem.detail}"
        )
        if problem.extensions:
            error_text += (
                " Extensions: "
                f"{json.dumps(problem.extensions, sort_keys=True, default=str)}"
            )

        super().__init__(
            error_text,
            response=response,
            request=response.request,
        )

    @property
    def type(self) -> str:
        """Return the problem type URI."""
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
        """Return the optional occurrence correlation URI."""
        return self.problem.instance

    @property
    def extensions(self) -> Mapping[str, Any]:
        """Return all Problem Details extension members."""
        return self.problem.extensions

    @property
    def code(self) -> Optional[str]:
        """Return the optional vBase code extension."""
        return self.problem.code

    @property
    def details(self) -> Optional[dict[str, Any]]:
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
