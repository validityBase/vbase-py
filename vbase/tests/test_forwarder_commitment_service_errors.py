"""Tests for Forwarder commitment service error propagation."""

# pylint: disable=protected-access

import json
import unittest
from pathlib import Path
from typing import Any, Dict, Union
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
    def _make_response(
        status_code: int,
        payload: Dict[str, Any],
        content_type: str = "application/problem+json; charset=utf-8",
    ) -> requests.Response:
        response = requests.Response()
        response.status_code = status_code
        response.url = "https://forwarder.example/execute"
        response.headers["Content-Type"] = content_type
        response._content = json.dumps(payload).encode("utf-8")
        response.request = requests.Request("POST", response.url).prepare()
        return response

    def _call_forwarder_with_response(
        self, response: requests.Response
    ) -> Union[Dict[str, Any], str, None]:
        """Call the test seam with a supplied Forwarder response."""
        default_user = patch.object(
            self.service, "get_default_user", return_value="0xuser"
        )
        forwarder_request = patch("requests.post", return_value=response)
        with default_user, forwarder_request:
            return self.service._call_forwarder_api(
                "execute",
                request_type=RequestType.POST,
            )

    @staticmethod
    def _problem_payload(**overrides: Any) -> Dict[str, Any]:
        # The .invalid host is a test placeholder, not a published vBase URL.
        payload = {
            "type": "https://example.invalid/problems/insufficient-credits",
            "title": "Insufficient Credits",
            "status": 402,
            "detail": "Insufficient credits to execute the request.",
            "instance": "urn:uuid:9bc21f1c-0acc-4e01-934d-d9b4bb75576e",
            "code": "INSUFFICIENT_CREDITS",
            "details": {
                "requiredCredits": 1,
                "availableCredits": 0,
            },
        }
        payload.update(overrides)
        return payload

    def test_structured_http_error_is_preserved(self):
        """Expose Problem Details while retaining requests compatibility."""
        response = self._make_response(402, self._problem_payload())

        with self.assertLogs(
            "vbase.core.forwarder_commitment_service", level="ERROR"
        ) as captured_logs:
            with self.assertRaises(ProblemDetailsError) as raised:
                self._call_forwarder_with_response(response)

        error = raised.exception
        self.assertEqual(
            error.type,
            "https://example.invalid/problems/insufficient-credits",
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
        self.assertIsInstance(error, requests.HTTPError)
        self.assertIn("INSUFFICIENT_CREDITS", str(error))
        self.assertNotIn("availableCredits", str(error))
        log_output = "\n".join(captured_logs.output)
        self.assertIn("INSUFFICIENT_CREDITS", log_output)
        self.assertNotIn("availableCredits", log_output)

    def test_missing_instance_is_allowed(self):
        """Allow the optional instance member to be omitted."""
        payload = self._problem_payload()
        del payload["instance"]
        response = self._make_response(402, payload)

        with self.assertRaises(ProblemDetailsError) as raised:
            self._call_forwarder_with_response(response)

        self.assertIsNone(raised.exception.instance)
        self.assertNotIn("instance", raised.exception.problem.to_dict())

    def test_public_constructor_rejects_missing_code(self):
        """Keep invalid vBase problem documents out of the public model."""
        with self.assertRaisesRegex(ValueError, "Invalid vBase Problem Details"):
            ProblemDetails(
                type="https://example.invalid/problems/bad-request",
                title="Bad Request",
                status=400,
                detail="Invalid request.",
            )

    def test_public_schema_matches_parser_contract(self):
        """Keep the published schema aligned with the SDK parser."""
        repository_root = Path(__file__).resolve().parents[2]
        schema_path = repository_root / "docs/problem-details.schema.json"
        with schema_path.open(encoding="utf-8") as schema_file:
            schema = json.load(schema_file)

        self.assertEqual(
            set(schema["required"]),
            {"type", "title", "status", "detail", "code"},
        )
        properties = schema["properties"]
        self.assertNotIn("$id", schema)
        self.assertEqual(properties["type"]["format"], "uri-reference")
        self.assertEqual(properties["instance"]["format"], "uri-reference")
        self.assertEqual(properties["status"]["minimum"], 100)
        self.assertEqual(properties["status"]["maximum"], 599)
        self.assertEqual(properties["code"]["pattern"], "^[A-Z][A-Z0-9_]{0,63}$")
        self.assertEqual(properties["details"]["type"], "object")

    def test_uri_reference_formats_are_supported(self):
        """Accept absolute, URN, network-path, and relative URI references."""
        valid_references = (
            "https://example.invalid/problems/bad-request",
            "urn:uuid:9bc21f1c-0acc-4e01-934d-d9b4bb75576e",
            "//example.invalid/problems/bad-request",
            "https://docs.vbase.com:99999/problems/bad-request",
            "https://[2001:db8::1]/problems/bad-request",
            "https://[::ffff:192.0.2.128]/problems/bad-request",
            "https://[v1.fe80]/problems/bad-request",
            "/problems/bad-request",
            "../problems/bad-request?source=sdk#request",
        )
        for reference in valid_references:
            with self.subTest(reference=reference):
                problem = ProblemDetails.from_dict(
                    {
                        "type": reference,
                        "title": "Bad Request",
                        "status": 400,
                        "detail": "Invalid request.",
                        "instance": reference,
                        "code": "BAD_REQUEST",
                    }
                )

                self.assertIsNotNone(problem)

    def test_malformed_uri_references_are_rejected(self):
        """Reject malformed raw URI-reference values without normalizing them."""
        invalid_references = (
            "not a valid URI reference",
            "https://example.com/%ZZ",
            "1invalid:scheme",
            "https://[invalid",
            "https://[invalid]",
            "https://[]",
            "https://[fe80::1%25eth0]",
            "https://[::ffff:256.1.2.3]/problems/bad-request",
            "https://[::ffff:192.168.1]/problems/bad-request",
            "https://[::ffff:192.168.001.1]/problems/bad-request",
            "https://example.com:invalid",
            "https://first@second@example.com/problem",
            "https://example.com/one#two#three",
            "https://example.com/проблема",
        )
        for reference in invalid_references:
            with self.subTest(reference=reference):
                problem = ProblemDetails.from_dict(
                    {
                        "type": reference,
                        "title": "Bad Request",
                        "status": 400,
                        "detail": "Invalid request.",
                        "code": "BAD_REQUEST",
                    }
                )

                self.assertIsNone(problem)

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
                    self._problem_payload(**invalid_member),
                )

                with self.assertRaises(requests.HTTPError) as raised:
                    self._call_forwarder_with_response(response)

                self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_invalid_status_is_rejected(self):
        """Reject booleans and values outside the HTTP status range."""
        for status in (True, 0, 99, 600):
            with self.subTest(status=status):
                problem = ProblemDetails.from_dict(self._problem_payload(status=status))

                self.assertIsNone(problem)

    def test_missing_or_invalid_code_remains_http_error(self):
        """Reject responses that do not satisfy the vBase extension contract."""
        for code in (None, "", 123, "lowercase", "BAD-CODE", "BAD\nFORGED"):
            with self.subTest(code=code):
                payload = self._problem_payload()
                if code is None:
                    del payload["code"]
                else:
                    payload["code"] = code
                response = self._make_response(402, payload)

                with self.assertRaises(requests.HTTPError) as raised:
                    self._call_forwarder_with_response(response)

                self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_success_false_response_raises_request_exception(self):
        """Reject an explicit application failure before extracting response data."""
        response = self._make_response(
            200,
            {"success": False, "log": "Forwarder execution failed."},
            content_type="application/json",
        )

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

    def test_non_problem_json_error_remains_http_error(self):
        """Do not interpret an arbitrary JSON error as Problem Details."""
        response = self._make_response(
            402,
            self._problem_payload(),
            content_type="application/json",
        )

        with self.assertRaises(requests.HTTPError) as raised:
            self._call_forwarder_with_response(response)

        self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_problem_status_mismatch_remains_http_error(self):
        """Reject a body whose status does not match the HTTP status line."""
        response = self._make_response(400, self._problem_payload())

        with self.assertRaises(requests.HTTPError) as raised:
            self._call_forwarder_with_response(response)

        self.assertNotIsInstance(raised.exception, ProblemDetailsError)

    def test_invalid_problem_json_remains_http_error(self):
        """Preserve the original error when the problem body is not JSON."""
        response = requests.Response()
        response.status_code = 500
        response.url = "https://forwarder.example/execute"
        response.headers["Content-Type"] = "application/problem+json"
        response._content = b"not-json"
        response.request = requests.Request("POST", response.url).prepare()

        with self.assertRaises(requests.HTTPError) as raised:
            self._call_forwarder_with_response(response)

        self.assertNotIsInstance(raised.exception, ProblemDetailsError)


if __name__ == "__main__":
    unittest.main()
