"""Tests for Forwarder commitment service error propagation."""

# pylint: disable=protected-access

import json
import unittest
from unittest.mock import patch

import requests

from vbase.core.forwarder_commitment_service import (
    ForwarderAPIError,
    ForwarderCommitmentService,
    RequestType,
)


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
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps(payload).encode("utf-8")
        response.request = requests.Request("POST", response.url).prepare()
        return response

    def test_structured_http_error_is_preserved(self):
        """Expose Forwarder error fields while retaining requests compatibility."""
        response = self._make_response(
            402,
            {
                "statusCode": 402,
                "code": "INSUFFICIENT_CREDITS",
                "message": "Insufficient credits to execute the request.",
                "details": {
                    "requiredCredits": 1,
                    "availableCredits": 0,
                },
            },
        )

        with (
            patch.object(self.service, "get_default_user", return_value="0xuser"),
            patch("requests.post", return_value=response),
        ):
            with self.assertRaises(ForwarderAPIError) as raised:
                self.service._call_forwarder_api(
                    "execute",
                    request_type=RequestType.POST,
                )

        error = raised.exception
        self.assertEqual(error.status_code, 402)
        self.assertEqual(error.code, "INSUFFICIENT_CREDITS")
        self.assertEqual(error.message, "Insufficient credits to execute the request.")
        self.assertEqual(
            error.details,
            {"requiredCredits": 1, "availableCredits": 0},
        )
        self.assertEqual(error.response_payload["statusCode"], 402)
        self.assertIs(error.response, response)
        self.assertIn("INSUFFICIENT_CREDITS", str(error))
        self.assertIn("Insufficient credits to execute the request.", str(error))
        self.assertIn('"availableCredits": 0', str(error))

    def test_unstructured_http_error_remains_requests_http_error(self):
        """Preserve legacy behavior for non-JSON upstream failures."""
        response = requests.Response()
        response.status_code = 502
        response.url = "https://forwarder.example/execute"
        response._content = b"Bad Gateway"
        response.request = requests.Request("POST", response.url).prepare()

        with (
            patch.object(self.service, "get_default_user", return_value="0xuser"),
            patch("requests.post", return_value=response),
        ):
            with self.assertRaises(requests.HTTPError) as raised:
                self.service._call_forwarder_api(
                    "execute",
                    request_type=RequestType.POST,
                )

        self.assertNotIsInstance(raised.exception, ForwarderAPIError)


if __name__ == "__main__":
    unittest.main()
