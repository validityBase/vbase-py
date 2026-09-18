"""Tests for Forwarder commitment service error propagation."""

# pylint: disable=protected-access

import json
import unittest
from unittest.mock import patch

import requests

from vbase.core.forwarder_commitment_service import (
    ForwarderCommitmentService,
    RequestType,
)
from vbase.core.problem_details import ProblemDetails, ProblemDetailsError


class TestForwarderCommitmentServiceErrors(unittest.TestCase):
    """Test structured Forwarder API failures."""

    def setUp(self):
        self.service = ForwarderCommitmentService.__new__(ForwarderCommitmentService)
        self.service.forwarder_url = "https://forwarder.example/"
        self.service.api_key = "test-api-key"

    @staticmethod
    def _make_response(status_code: int, payload: dict) -> requests.Response:
        response = requests.Response()
        response.status_code = status_code
        response.url = "https://forwarder.example/execute"
        response.headers["Content-Type"] = "application/problem+json; charset=utf-8"
        response._content = json.dumps(payload).encode("utf-8")
        response.request = requests.Request("POST", response.url).prepare()
        return response

    def _call_forwarder_with_response(
        self, response: requests.Response
    ) -> dict | str | None:
        """Call the test seam with a supplied Forwarder response."""
        with (
            patch.object(self.service, "get_default_user", return_value="0xuser"),
            patch("requests.post", return_value=response),
        ):
            return self.service._call_forwarder_api(
                "execute",
                request_type=RequestType.POST,
            )

    def test_structured_http_error_is_preserved(self):
        """Expose Forwarder error fields while retaining requests compatibility."""
        response = self._make_response(
            402,
            {
                "type": "https://docs.vbase.com/problems/insufficient-credits",
                "title": "Insufficient Credits",
                "status": 402,
                "detail": "Insufficient credits to execute the request.",
                "instance": "urn:uuid:9bc21f1c-0acc-4e01-934d-d9b4bb75576e",
                "code": "INSUFFICIENT_CREDITS",
                "details": {
                    "requiredCredits": 1,
                    "availableCredits": 0,
                },
            },
        )

        with self.assertRaises(ProblemDetailsError) as raised:
            self._call_forwarder_with_response(response)

        error = raised.exception
        self.assertEqual(
            error.type,
            "https://docs.vbase.com/problems/insufficient-credits",
        )
        self.assertEqual(error.title, "Insufficient Credits")
        self.assertEqual(error.status, 402)
        self.assertEqual(error.code, "INSUFFICIENT_CREDITS")
        self.assertEqual(error.detail, "Insufficient credits to execute the request.")
        self.assertEqual(
            error.instance,
            "urn:uuid:9bc21f1c-0acc-4e01-934d-d9b4bb75576e",
        )
        self.assertEqual(
            error.details,
            {"requiredCredits": 1, "availableCredits": 0},
        )
        self.assertEqual(error.extensions["code"], "INSUFFICIENT_CREDITS")
        self.assertEqual(error.problem.to_dict()["status"], 402)
        self.assertIs(error.response, response)
        self.assertIn("INSUFFICIENT_CREDITS", str(error))
        self.assertIn("Insufficient credits to execute the request.", str(error))
        self.assertIn('"availableCredits": 0', str(error))

    def test_problem_details_adds_extensions_without_mutating_standard_members(self):
        """Expose a reusable immutable model for downstream HTTP adapters."""
        problem = ProblemDetails(
            type="https://docs.vbase.com/problems/bad-request",
            title="Bad Request",
            status=400,
            detail="Invalid request.",
            extensions={"code": "BAD_REQUEST"},
        )

        extended = problem.with_extensions(
            user_message="Correct the request and try again.",
            status=500,
        )

        self.assertEqual(problem.extensions, {"code": "BAD_REQUEST"})
        self.assertEqual(extended.status, 400)
        self.assertEqual(
            extended.extensions,
            {
                "code": "BAD_REQUEST",
                "user_message": "Correct the request and try again.",
            },
        )

    def test_missing_instance_is_allowed(self):
        """Allow the optional instance member to be omitted."""
        response = self._make_response(
            402,
            {
                "type": "https://docs.vbase.com/problems/insufficient-credits",
                "title": "Insufficient Credits",
                "status": 402,
                "detail": "Insufficient credits to execute the request.",
                "code": "INSUFFICIENT_CREDITS",
            },
        )

        with self.assertRaises(ProblemDetailsError) as raised:
            self._call_forwarder_with_response(response)

        self.assertIsNone(raised.exception.instance)
        self.assertNotIn("instance", raised.exception.problem.to_dict())

    def test_invalid_optional_members_remain_http_error(self):
        """Reject optional members whose values violate the public schema."""
        invalid_members = (
            {"instance": None},
            {"instance": 123},
            {"instance": "not a valid URI reference"},
            {"details": None},
            {"details": "invalid"},
        )
        for invalid_member in invalid_members:
            with self.subTest(invalid_member=invalid_member):
                response = self._make_response(
                    402,
                    {
                        "type": "https://docs.vbase.com/problems/insufficient-credits",
                        "title": "Insufficient Credits",
                        "status": 402,
                        "detail": "Insufficient credits to execute the request.",
                        "code": "INSUFFICIENT_CREDITS",
                        **invalid_member,
                    },
                )

                with self.assertRaises(requests.HTTPError) as raised:
                    self._call_forwarder_with_response(response)

                self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_invalid_type_uri_reference_remains_http_error(self):
        """Reject a problem type whose value is not a URI reference."""
        response = self._make_response(
            400,
            {
                "type": "not a valid URI reference",
                "title": "Bad Request",
                "status": 400,
                "detail": "Invalid request.",
                "code": "BAD_REQUEST",
            },
        )

        with self.assertRaises(requests.HTTPError) as raised:
            self._call_forwarder_with_response(response)

        self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_relative_uri_references_are_allowed(self):
        """Accept relative URI references permitted by RFC 9457."""
        problem = ProblemDetails.from_dict(
            {
                "type": "/problems/bad-request",
                "title": "Bad Request",
                "status": 400,
                "detail": "Invalid request.",
                "instance": "/problems/instances/123",
                "code": "BAD_REQUEST",
            }
        )

        self.assertIsNotNone(problem)
        self.assertEqual(problem.type, "/problems/bad-request")
        self.assertEqual(problem.instance, "/problems/instances/123")

    def test_out_of_range_status_is_rejected(self):
        """Reject status values outside the public schema's HTTP range."""
        for status in (0, 99, 600):
            with self.subTest(status=status):
                problem = ProblemDetails.from_dict(
                    {
                        "type": "https://docs.vbase.com/problems/invalid-status",
                        "title": "Invalid Status",
                        "status": status,
                        "detail": "Invalid status.",
                        "code": "INVALID_STATUS",
                    }
                )

                self.assertIsNone(problem)

    def test_missing_or_invalid_code_remains_http_error(self):
        """Reject responses that do not satisfy the vBase extension contract."""
        for code in (None, "", 123):
            with self.subTest(code=code):
                payload: dict[str, object] = {
                    "type": "https://docs.vbase.com/problems/invalid-api-key",
                    "title": "Invalid API Key",
                    "status": 401,
                    "detail": "Invalid API key.",
                }
                if code is not None:
                    payload["code"] = code
                response = self._make_response(401, payload)

                with self.assertRaises(requests.HTTPError) as raised:
                    self._call_forwarder_with_response(response)

                self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_success_false_response_raises_request_exception(self):
        """Reject an explicit application failure before extracting response data."""
        response = self._make_response(
            200,
            {
                "success": False,
                "log": "Forwarder execution failed.",
            },
        )
        response.headers["Content-Type"] = "application/json"

        with self.assertRaisesRegex(
            requests.RequestException,
            "Forwarder execution failed.",
        ):
            self._call_forwarder_with_response(response)

    def test_unstructured_http_error_remains_requests_http_error(self):
        """Preserve requests behavior for non-JSON upstream failures."""
        response = requests.Response()
        response.status_code = 502
        response.url = "https://forwarder.example/execute"
        response._content = b"Bad Gateway"
        response.request = requests.Request("POST", response.url).prepare()

        with self.assertRaises(requests.HTTPError) as raised:
            self._call_forwarder_with_response(response)

        self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_json_error_without_problem_media_type_remains_http_error(self):
        """Do not interpret an arbitrary JSON error as Problem Details."""
        response = self._make_response(
            401,
            {
                "type": "https://docs.vbase.com/problems/invalid-api-key",
                "title": "Invalid API Key",
                "status": 401,
                "detail": "Invalid API key.",
            },
        )
        response.headers["Content-Type"] = "application/json"

        with self.assertRaises(requests.HTTPError) as raised:
            self._call_forwarder_with_response(response)

        self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_problem_status_mismatch_remains_http_error(self):
        """Reject a body whose status does not match the HTTP status line."""
        response = self._make_response(
            400,
            {
                "type": "https://docs.vbase.com/problems/insufficient-credits",
                "title": "Insufficient Credits",
                "status": 402,
                "detail": "Mismatched status.",
                "code": "INSUFFICIENT_CREDITS",
            },
        )

        with self.assertRaises(requests.HTTPError) as raised:
            self._call_forwarder_with_response(response)

        self.assertNotIsInstance(raised.exception, ProblemDetailsError)


if __name__ == "__main__":
    unittest.main()
