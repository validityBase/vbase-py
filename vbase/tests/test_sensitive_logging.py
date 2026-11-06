"""Unit tests for sensitive-value logging safeguards."""

# pylint: disable=too-many-public-methods

import json
import os
import time
import unittest
from unittest.mock import Mock, patch

import requests
from web3.exceptions import InvalidAddress, Web3RPCError

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.indexing_service import Web3HTTPIndexingService
from vbase.core.problem_details import ProblemDetailsError
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.utils.log import REDACTED_LOG_VALUE, mask_api_key, redact_url, redact_urls

_RPC_SECRET = "private-rpc-token"
_RPC_URL = f"https://rpc.example/v2/{_RPC_SECRET}"
_REDACTED_RPC_URL = f"https://rpc.example/{REDACTED_LOG_VALUE}"
# Requests accepts these passwords; the delimiters must not end redaction early.
_RPC_PASSWORD = "pa'ss\"word"
_RPC_URLS_WITH_CREDENTIALS = (
    _RPC_URL,
    f"https://user:pa'ss@rpc.example/v2/{_RPC_SECRET}",
    f"https://user:{_RPC_PASSWORD}@rpc.example/v2/{_RPC_SECRET}?key=k'ey",
)
# Requests also accepts whitespace in a configured URL's credentials.
_CONFIGURED_RPC_URLS = (
    *_RPC_URLS_WITH_CREDENTIALS,
    f"https://user:prefix credential-tail@rpc.example/v2/{_RPC_SECRET}",
)


def _problem_response(type_uri: str, detail: str) -> requests.Response:
    """Build a forwarder Problem Details response with the given members."""
    response = requests.Response()
    response.status_code = 402
    response.url = "https://forwarder.example/commitment-service-data"
    response.headers["Content-Type"] = "application/problem+json"
    response._content = json.dumps(  # pylint: disable=protected-access
        {
            "type": type_uri,
            "title": "Insufficient Credits",
            "status": 402,
            "detail": detail,
            "code": "INSUFFICIENT_CREDITS",
        }
    ).encode("utf-8")
    response.request = requests.Request("GET", response.url).prepare()
    return response


def _get_commitment_service_data() -> dict:
    """Call the forwarder's commitment-service-data API with a mocked request."""
    service = object.__new__(ForwarderCommitmentService)
    service.forwarder_url = "https://forwarder.example/"
    service.api_key = "test-api-key"
    with patch.object(service, "get_default_user", return_value="0x" + "11" * 20):
        return service.get_commitment_service_data()


class TestSensitiveLogging(unittest.TestCase):
    """Verify credentials cannot be emitted by environment diagnostics."""

    def test_mask_api_key_keeps_only_safe_prefix_and_suffix(self):
        """Long API keys retain only eight characters at each end."""
        api_key = "qHhlk9M6-middle-secretX-2fckvz-A"
        self.assertEqual(len(api_key), 32)

        self.assertEqual(
            mask_api_key(api_key), "qHhlk9M6\N{HORIZONTAL ELLIPSIS}2fckvz-A"
        )

    def test_mask_api_key_fully_redacts_short_keys(self):
        """Short API keys are not partially disclosed."""
        self.assertEqual(mask_api_key("short-api-key"), REDACTED_LOG_VALUE)
        for length in (16, 17, 31):
            with self.subTest(length=length):
                self.assertEqual(mask_api_key("x" * length), REDACTED_LOG_VALUE)
        self.assertIsNone(mask_api_key(None))
        self.assertEqual(mask_api_key(""), "")

    def test_redact_urls_keeps_only_scheme_and_host(self):
        """URL credentials, paths, queries, and fragments are redacted."""
        cases = {
            _RPC_URL: _REDACTED_RPC_URL,
            "https://user:pass@rpc.example:8545/v3/key?token=t#f": (
                f"https://rpc.example:8545/{REDACTED_LOG_VALUE}"
            ),
            "wss://rpc.example/ws?key=secret": f"wss://rpc.example/{REDACTED_LOG_VALUE}",
            "http://127.0.0.1:8545": "http://127.0.0.1:8545",
            "https://rpc.example/": "https://rpc.example/",
            "no URL 50%": "no URL 50%",
            **{url: _REDACTED_RPC_URL for url in _RPC_URLS_WITH_CREDENTIALS},
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(redact_urls(text), expected)
                self.assertEqual(redact_urls(expected), expected)

    def test_redact_url_treats_value_as_one_url(self):
        """A configured URL is redacted in full, even with whitespace in it."""
        cases = {
            **{url: _REDACTED_RPC_URL for url in _CONFIGURED_RPC_URLS},
            f"{_RPC_URL}\n": _REDACTED_RPC_URL,
            "https://user:pass word@rpc.example:8545": "https://rpc.example:8545",
            "http://127.0.0.1:8545": "http://127.0.0.1:8545",
            "user:pass word@rpc.example/v2/key": REDACTED_LOG_VALUE,
        }
        for url, expected in cases.items():
            with self.subTest(url=url):
                self.assertEqual(redact_url(url), expected)
        # An unset URL selects web3's default local endpoint.
        self.assertIsNone(redact_url(None))
        self.assertEqual(redact_url(""), "")

    def test_redact_urls_trailing_punctuation(self):
        """Trailing punctuation is redacted unless it closes a quote or bracket.

        Punctuation can be a whole credential, so it is surrounding text only
        when it starts at the closer for the character before the URL.
        """
        cases = {
            f"connecting to {_RPC_URL}: retrying.": (
                f"connecting to {_REDACTED_RPC_URL} retrying."
            ),
            f"retrying ({_RPC_URL}).": f"retrying ({_REDACTED_RPC_URL}).",
            f"Invalid URL '{_RPC_URLS_WITH_CREDENTIALS[1]}': No host": (
                f"Invalid URL '{_REDACTED_RPC_URL}': No host"
            ),
            "https://rpc.example/auth?password=!!!": _REDACTED_RPC_URL,
            "'https://rpc.example/v2/!!!'": f"'{_REDACTED_RPC_URL}'",
            "with url: /!!! (Caused by X)": (
                f"with url: /{REDACTED_LOG_VALUE} (Caused by X)"
            ),
            "with url: /v2/key!! (Caused by X)": (
                f"with url: /{REDACTED_LOG_VALUE} (Caused by X)"
            ),
            # A host has no credentials to redact, so its punctuation is kept.
            "Connected to http://127.0.0.1:8545.": "Connected to http://127.0.0.1:8545.",
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(redact_urls(text), expected)
                self.assertEqual(redact_urls(expected), expected)

    def test_redact_urls_whitespace_in_userinfo(self):
        """A URL with whitespace before its "@" is reduced to its scheme."""
        cases = {
            f"Node {_CONFIGURED_RPC_URLS[-1]} is down.": (
                f"Node https://{REDACTED_LOG_VALUE} is down."
            ),
            f"Node '{_CONFIGURED_RPC_URLS[-1]}' is down.": (
                f"Node 'https://{REDACTED_LOG_VALUE}' is down."
            ),
            # Unrelated text up to an "@" is redacted, because it cannot be
            # told apart from credentials.
            "Use https://rpc.example or mail ops@corp.example": (
                f"Use https://{REDACTED_LOG_VALUE}"
            ),
            # A "/", "?", or "#" ends the userinfo, so later text is kept.
            "Use https://rpc.example/ or mail ops@corp.example": (
                "Use https://rpc.example/ or mail ops@corp.example"
            ),
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(redact_urls(text), expected)
                self.assertEqual(redact_urls(expected), expected)

    def test_redact_urls_long_text_is_linear(self):
        """Long dotted or hyphenated text is redacted in linear time."""
        for separator in ".-+":
            text = f"a{separator}" * 50_000 + _RPC_URL
            with self.subTest(separator=separator):
                start = time.perf_counter()
                redacted = redact_urls(text)
                self.assertLess(time.perf_counter() - start, 5)
                self.assertNotIn(_RPC_SECRET, redacted)

    def test_redact_urls_handles_requests_error_messages(self):
        """Error text from requests keeps its diagnostics without credentials."""
        http_error = f"401 Client Error: Unauthorized for url: {_RPC_URL}"
        connection_error = (
            "HTTPSConnectionPool(host='rpc.example', port=443): Max retries "
            f"exceeded with url: /v2/{_RPC_SECRET}?key=k'ey "
            "(Caused by NewConnectionError())"
        )

        self.assertEqual(
            redact_urls(requests.HTTPError(http_error)),
            f"401 Client Error: Unauthorized for url: {_REDACTED_RPC_URL}",
        )
        self.assertEqual(
            redact_urls(connection_error),
            "HTTPSConnectionPool(host='rpc.example', port=443): Max retries "
            f"exceeded with url: /{REDACTED_LOG_VALUE} (Caused by NewConnectionError())",
        )

    @patch("vbase.core.forwarder_commitment_service._LOG.debug")
    def test_forwarder_environment_log_masks_credentials(self, debug_mock):
        """Forwarder diagnostics mask API keys and omit private values."""
        api_key = "qHhlk9M6-middle-secretX-2fckvz-A"
        private_key = "0x" + "11" * 32
        forwarder_url = "https://forwarder.example/path?token=forwarder-secret"
        environment = {
            "VBASE_FORWARDER_URL": forwarder_url,
            "VBASE_API_KEY": api_key,
            "VBASE_COMMITMENT_SERVICE_PRIVATE_KEY": private_key,
        }

        with patch.dict(os.environ, environment):
            init_args = ForwarderCommitmentService.get_init_args_from_env()

        self.assertEqual(init_args["forwarder_url"], forwarder_url)
        self.assertEqual(init_args["api_key"], api_key)
        self.assertEqual(init_args["private_key"], private_key)
        safe_log_args = debug_mock.call_args.args[1]
        self.assertEqual(
            safe_log_args,
            {
                "forwarder_url_configured": True,
                "api_key": "qHhlk9M6\N{HORIZONTAL ELLIPSIS}2fckvz-A",
                "private_key_configured": True,
            },
        )
        rendered_call = repr(debug_mock.call_args)
        self.assertNotIn(api_key, rendered_call)
        self.assertNotIn(private_key, rendered_call)
        self.assertNotIn(forwarder_url, rendered_call)

    @patch("vbase.core.forwarder_commitment_service._LOG.debug")
    def test_forwarder_environment_log_fully_redacts_17_character_key(self, debug_mock):
        """A short configured API key must never be nearly exposed."""
        api_key = "x" * 17
        environment = {
            "VBASE_FORWARDER_URL": "https://forwarder.example",
            "VBASE_API_KEY": api_key,
            "VBASE_COMMITMENT_SERVICE_PRIVATE_KEY": "0x" + "11" * 32,
        }

        with patch.dict(os.environ, environment):
            init_args = ForwarderCommitmentService.get_init_args_from_env()

        self.assertEqual(init_args["api_key"], api_key)
        self.assertEqual(debug_mock.call_args.args[1]["api_key"], REDACTED_LOG_VALUE)
        self.assertNotIn(api_key, repr(debug_mock.call_args))

    @patch("vbase.core.web3_http_commitment_service._LOG.debug")
    def test_web3_environment_log_omits_private_rpc_values(self, debug_mock):
        """Web3 diagnostics omit private keys and complete RPC URLs."""
        private_key = "0x" + "22" * 32
        node_rpc_url = "https://rpc.example/v2/rpc-provider-api-key"
        service_address = "0x" + "33" * 20
        environment = {
            "VBASE_COMMITMENT_SERVICE_NODE_RPC_URL": node_rpc_url,
            "VBASE_COMMITMENT_SERVICE_ADDRESS": service_address,
            "VBASE_COMMITMENT_SERVICE_PRIVATE_KEY": private_key,
        }

        with patch.dict(os.environ, environment):
            init_args = Web3HTTPCommitmentService.get_init_args_from_env()

        self.assertEqual(init_args["node_rpc_url"], node_rpc_url)
        self.assertEqual(init_args["private_key"], private_key)
        safe_log_args = debug_mock.call_args.args[1]
        self.assertEqual(safe_log_args["node_rpc_url_configured"], True)
        self.assertEqual(safe_log_args["commitment_service_address"], service_address)
        self.assertEqual(safe_log_args["private_key_configured"], True)
        rendered_call = repr(debug_mock.call_args)
        self.assertNotIn(node_rpc_url, rendered_call)
        self.assertNotIn(private_key, rendered_call)

    def test_web3_environment_reads_poa_middleware_flag(self):
        """The POA middleware flag is read from its environment variable."""
        environment = {
            "VBASE_COMMITMENT_SERVICE_NODE_RPC_URL": _RPC_URL,
            "VBASE_COMMITMENT_SERVICE_ADDRESS": "0x" + "33" * 20,
            "VBASE_COMMITMENT_SERVICE_PRIVATE_KEY": "0x" + "22" * 32,
        }
        for value, expected in (("true", True), ("1", True), ("false", False)):
            with self.subTest(value=value):
                with patch.dict(
                    os.environ,
                    {
                        **environment,
                        "VBASE_COMMITMENT_SERVICE_INJECT_GETH_POA_MIDDLEWARE": value,
                    },
                ):
                    init_args = Web3HTTPCommitmentService.get_init_args_from_env()
                self.assertIs(init_args["inject_geth_poa_middleware"], expected)

        with patch.dict(os.environ, environment):
            os.environ.pop("VBASE_COMMITMENT_SERVICE_INJECT_GETH_POA_MIDDLEWARE", None)
            init_args = Web3HTTPCommitmentService.get_init_args_from_env()
        self.assertIs(init_args["inject_geth_poa_middleware"], False)

    @patch("vbase.core.web3_http_commitment_service.time.sleep")
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_MAX_RETRIES", 1)
    @patch("vbase.core.web3_http_commitment_service.Web3")
    def test_web3_connection_failure_redacts_rpc_url(self, web3_mock, _sleep_mock):
        """Connection errors name the node host without its credentials."""
        web3_mock.return_value.is_connected.return_value = False

        for node_rpc_url in _CONFIGURED_RPC_URLS:
            with self.subTest(node_rpc_url=node_rpc_url):
                with self.assertLogs(
                    "vbase.core.web3_http_commitment_service", level="ERROR"
                ) as captured:
                    with self.assertRaises(ConnectionError) as raised:
                        Web3HTTPCommitmentService(
                            node_rpc_url=node_rpc_url,
                            commitment_service_address="0x" + "33" * 20,
                        )

                self.assertEqual(
                    str(raised.exception),
                    f"Failed to connect to {_REDACTED_RPC_URL} after 1 retries",
                )
                log_output = "\n".join(captured.output)
                self.assertIn(
                    f"Exception connecting to {_REDACTED_RPC_URL}", log_output
                )
                for secret in (_RPC_SECRET, "user:", "credential-tail"):
                    self.assertNotIn(secret, log_output)
                    self.assertNotIn(secret, str(raised.exception))

    def test_indexing_descriptor_log_omits_private_values(self):
        """Indexer configuration logs only a safe count."""
        node_rpc_url = "https://rpc.example/v2/private-rpc-token"
        private_key = "0x" + "44" * 32
        init_args = {
            "node_rpc_url": node_rpc_url,
            "commitment_service_address": "0x" + "33" * 20,
            "private_key": private_key,
        }
        descriptor = {
            "commitment_services": [
                {"class": "Web3HTTPCommitmentService", "init_args": init_args}
            ]
        }
        environment = {"VBASE_INDEXING_SERVICE_JSON_DESCRIPTOR": json.dumps(descriptor)}

        with (
            patch.dict(os.environ, environment),
            patch("vbase.core.indexing_service._LOG.info") as info_mock,
            patch(
                "vbase.core.indexing_service.Web3HTTPCommitmentService"
            ) as service_mock,
        ):
            indexer = Web3HTTPIndexingService.create_instance_from_env_json_descriptor()

        self.assertEqual(indexer.commitment_services, [service_mock.return_value])
        service_mock.assert_called_once_with(**init_args)
        info_mock.assert_called_once_with(
            "Initializing indexing service. commitment_service_count=%d", 1
        )
        self.assertNotIn(node_rpc_url, repr(info_mock.call_args_list))
        self.assertNotIn(private_key, repr(info_mock.call_args_list))

    @patch("vbase.core.forwarder_commitment_service.requests.get")
    def test_forwarder_http_error_log_redacts_url(self, get_mock):
        """HTTP failures log the API, status, and error without URL credentials."""
        secret = "private-forwarder-token"
        service = object.__new__(ForwarderCommitmentService)
        service.forwarder_url = f"https://forwarder.example/?token={secret}"
        service.api_key = "test-api-key"
        response = Mock(status_code=502)
        get_mock.return_value.raise_for_status.side_effect = requests.HTTPError(
            f"502 Server Error: Bad Gateway for url: {service.forwarder_url}",
            response=response,
        )

        with patch(
            "vbase.core.forwarder_commitment_service.ProblemDetailsError.from_response",
            return_value=None,
        ):
            with self.assertLogs(
                "vbase.core.forwarder_commitment_service", level="ERROR"
            ) as captured:
                with patch.object(
                    service, "get_default_user", return_value="0x" + "11" * 20
                ):
                    with self.assertRaises(requests.HTTPError):
                        service.get_commitment_service_data()

        self.assertEqual(
            captured.records[0].getMessage(),
            "HTTP error occurred: api=commitment-service-data status=502 "
            "error=502 Server Error: Bad Gateway for url: "
            f"https://forwarder.example/{REDACTED_LOG_VALUE}",
        )
        self.assertNotIn(secret, "\n".join(captured.output))

    @patch("vbase.core.forwarder_commitment_service.requests.get")
    def test_structured_forwarder_error_omits_untrusted_type(self, get_mock):
        """A problem type cannot copy URL credentials into logs or messages."""
        secret = "private-forwarder-token"
        get_mock.return_value = _problem_response(
            f"https://forwarder.example/problems/credits?token={secret}",
            "Insufficient credits.",
        )

        with self.assertLogs(
            "vbase.core.forwarder_commitment_service", level="ERROR"
        ) as captured:
            with self.assertRaises(ProblemDetailsError) as raised:
                _get_commitment_service_data()

        self.assertEqual(
            captured.records[0].getMessage(),
            "HTTP error occurred: api=commitment-service-data status=402 "
            "error=HTTP API problem (402) code=INSUFFICIENT_CREDITS: "
            "Insufficient credits.",
        )
        self.assertNotIn(secret, str(raised.exception))
        self.assertIn(secret, raised.exception.type)

    @patch("vbase.core.forwarder_commitment_service.requests.get")
    def test_structured_forwarder_error_redacts_detail_urls(self, get_mock):
        """A problem detail cannot copy URL credentials into logs or messages."""
        detail = f"Node {_CONFIGURED_RPC_URLS[-1]} is down."
        redacted_message = (
            "HTTP API problem (402) code=INSUFFICIENT_CREDITS: "
            f"Node https://{REDACTED_LOG_VALUE} is down."
        )
        response = _problem_response("https://forwarder.example/problems/x", detail)
        get_mock.return_value = response

        error = ProblemDetailsError.from_response(response)
        with self.assertLogs(
            "vbase.core.forwarder_commitment_service", level="ERROR"
        ) as captured:
            with self.assertRaises(ProblemDetailsError) as raised:
                _get_commitment_service_data()

        self.assertEqual(str(error), redacted_message)
        self.assertEqual(str(raised.exception), redacted_message)
        self.assertEqual(
            captured.records[0].getMessage(),
            "HTTP error occurred: api=commitment-service-data status=402 "
            f"error={redacted_message}",
        )
        self.assertEqual(error.detail, detail)

    @patch("vbase.core.forwarder_commitment_service.requests.get")
    def test_forwarder_transport_error_log_redacts_url(self, get_mock):
        """Transport failures log the API and error without URL credentials."""
        secret = "private-forwarder-token"
        service = object.__new__(ForwarderCommitmentService)
        service.api_key = "test-api-key"

        for forwarder_url in (
            f"https://forwarder.example/?token={secret}",
            f"https://user:pa'ss@forwarder.example/?token=a'{secret}",
        ):
            with self.subTest(forwarder_url=forwarder_url):
                service.forwarder_url = forwarder_url
                get_mock.side_effect = requests.ConnectionError(forwarder_url)

                with self.assertLogs(
                    "vbase.core.forwarder_commitment_service", level="ERROR"
                ) as captured:
                    with patch.object(
                        service, "get_default_user", return_value="0x" + "11" * 20
                    ):
                        with self.assertRaises(requests.ConnectionError):
                            service.get_commitment_service_data()

                self.assertEqual(
                    captured.records[0].getMessage(),
                    "Request error occurred: api=commitment-service-data "
                    f"error=https://forwarder.example/{REDACTED_LOG_VALUE}",
                )

    @patch("vbase.core.web3_http_commitment_service.time.sleep")
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_MAX_RETRIES", 1)
    @patch("vbase.core.web3_http_commitment_service.Web3")
    def test_web3_connection_exception_log_redacts_rpc_url(
        self, web3_mock, _sleep_mock
    ):
        """Provider errors may include a credential-bearing RPC URL."""
        web3_mock.return_value.is_connected.side_effect = (
            ConnectionError(_RPC_URL),
            False,
        )

        with self.assertLogs(
            "vbase.core.web3_http_commitment_service", level="ERROR"
        ) as captured:
            with self.assertRaises(ConnectionError) as raised:
                Web3HTTPCommitmentService(
                    node_rpc_url=_RPC_URL,
                    commitment_service_address="0x" + "33" * 20,
                )

        self.assertEqual(
            captured.records[0].getMessage(),
            "Web3HTTPCommitmentService.__init__(): Exception connecting to "
            f"{_REDACTED_RPC_URL}: {_REDACTED_RPC_URL}",
        )
        self.assertNotIn(_RPC_SECRET, str(raised.exception))

    @patch("retry.api.time.sleep")
    def test_indexing_retry_logs_redacted_rpc_url(self, _sleep_mock):
        """A transient provider error is logged without its URL credentials."""
        event = Mock()
        event.get_logs.side_effect = [ConnectionError(_RPC_URL), []]
        indexer = Web3HTTPIndexingService([])

        with self.assertLogs(
            "vbase.core.indexing_service", level="WARNING"
        ) as captured:
            # pylint: disable-next=protected-access
            self.assertEqual(indexer._get_event_logs(event, 5, {"user": "0xabc"}), [])

        self.assertEqual(event.get_logs.call_count, 2)
        event.get_logs.assert_called_with(
            argument_filters={"user": "0xabc"}, from_block=5, to_block="latest"
        )
        # The retry library appends ", retrying"; a comma directly after a URL
        # path can belong to a credential, so it is redacted with the path.
        self.assertEqual(
            captured.records[0].getMessage(),
            f"{_REDACTED_RPC_URL} retrying in 2 seconds...",
        )

    @patch("retry.api.time.sleep")
    def test_indexing_query_retries_rpc_errors(self, _sleep_mock):
        """A JSON-RPC error response from the node is retried."""
        event = Mock()
        event.get_logs.side_effect = [Web3RPCError("query timeout exceeded"), []]
        indexer = Web3HTTPIndexingService([])

        # pylint: disable-next=protected-access
        self.assertEqual(indexer._get_event_logs(event, 0, {"user": "0xabc"}), [])

        self.assertEqual(event.get_logs.call_count, 2)

    def test_indexing_query_does_not_retry_invalid_arguments(self):
        """Invalid query arguments fail immediately instead of being retried."""
        # InvalidAddress is a Web3Exception raised before any request is sent.
        for error in (InvalidAddress("invalid address"), ValueError("invalid")):
            with self.subTest(error=type(error).__name__):
                event = Mock()
                event.get_logs.side_effect = error
                indexer = Web3HTTPIndexingService([])

                with self.assertRaises(type(error)):
                    # pylint: disable-next=protected-access
                    indexer._get_event_logs(event, 0, {"user": "0xuser"})

                self.assertEqual(event.get_logs.call_count, 1)

    @patch("retry.api.time.sleep")
    def test_set_existence_retry_logs_redacted_rpc_url(self, _sleep_mock):
        """A transient set lookup error is logged without its URL credentials."""
        service = object.__new__(Web3HTTPCommitmentService)
        with patch.object(
            service,
            "user_set_exists",
            side_effect=[ConnectionError(_RPC_URL), True],
        ) as exists_mock:
            with self.assertLogs(
                "vbase.core.web3_commitment_service", level="WARNING"
            ) as captured:
                # pylint: disable-next=protected-access
                self.assertIsNone(service._user_set_exists_with_retry("0xabc", "0xdef"))

        self.assertEqual(exists_mock.call_count, 2)
        self.assertEqual(
            captured.records[0].getMessage(),
            f"{_REDACTED_RPC_URL} retrying in 1 seconds...",
        )

    @patch("vbase.core.web3_http_commitment_service.time.sleep")
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_MAX_RETRIES", 2)
    @patch("vbase.core.web3_http_commitment_service.Web3")
    def test_web3_repeated_connection_exceptions_redact_rpc_url(
        self, web3_mock, _sleep_mock
    ):
        """Each failed attempt is logged; none exposes the URL credentials."""
        web3_mock.return_value.is_connected.side_effect = ConnectionError(_RPC_URL)

        with self.assertLogs(
            "vbase.core.web3_http_commitment_service", level="ERROR"
        ) as captured:
            with self.assertRaises(ConnectionError) as raised:
                Web3HTTPCommitmentService(
                    node_rpc_url=_RPC_URL,
                    commitment_service_address="0x" + "33" * 20,
                )

        self.assertEqual(web3_mock.return_value.is_connected.call_count, 2)
        self.assertEqual(len(captured.records), 2)
        self.assertNotIn(_RPC_SECRET, str(raised.exception))
        self.assertNotIn(_RPC_SECRET, "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main()
