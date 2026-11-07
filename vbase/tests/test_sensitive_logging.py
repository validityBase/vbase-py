"""Unit tests for sensitive-value logging safeguards."""

# pylint: disable=too-many-public-methods,too-many-lines

import io
import json
import logging
import os
import pickle
import time
import traceback
import unittest
from unittest.mock import Mock, patch

import requests
from eth_abi import encode
from urllib3.exceptions import MaxRetryError
from web3 import Web3
from web3.exceptions import (
    BadResponseFormat,
    ContractLogicError,
    InvalidAddress,
    Web3RPCError,
)

from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.indexing_service import Web3HTTPIndexingService
from vbase.core.problem_details import ProblemDetailsError
from vbase.core.web3_http_commitment_service import (
    Web3HTTPCommitmentService,
    _RedactingHTTPProvider,
)
from vbase.utils.log import (
    REDACTED_LOG_VALUE,
    RedactUrlsFilter,
    mask_api_key,
    redact_exception,
    redact_url,
    redact_urls,
)

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


def _traceback_text(error: BaseException) -> str:
    """Format an exception as a standard traceback, including its chain."""
    return "".join(traceback.format_exception(error))


def _connection_error(url_path: str) -> requests.ConnectionError:
    """Build a requests connection error that wraps a urllib3 error, as requests does."""
    max_retry_error = MaxRetryError(None, url_path, OSError("Connection refused"))
    try:
        raise max_retry_error
    except MaxRetryError as error:
        try:
            raise requests.ConnectionError(error) from error
        except requests.ConnectionError as connection_error:
            return connection_error


def _node_response(content: bytes) -> requests.Response:
    """Build a successful node HTTP response with the given body."""
    response = requests.Response()
    response.status_code = 200
    response._content = content  # pylint: disable=protected-access
    response.raw = io.BytesIO(b"")
    return response


def _revert_post(message: str, data: str):
    """Build a node request mock that answers eth_call with a revert error."""

    def post(*_args, **kwargs) -> requests.Response:
        body: dict = {"jsonrpc": "2.0", "id": 0}
        if json.loads(kwargs["data"])["method"] == "eth_chainId":
            body["result"] = "0x1"
        else:
            body["error"] = {"code": 3, "message": message, "data": data}
        return _node_response(json.dumps(body).encode())

    return post


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

    def test_redact_url_keeps_only_scheme_of_malformed_url(self):
        """Text in the host position of a malformed URL can be credentials."""
        # An unencoded "/", "?", or "#" in a password ends the host part early,
        # and a password whose "@host" is missing is in the port's position.
        for url in (
            f"https://user:{_RPC_SECRET}/x@rpc.example/v2/key",
            f"https://user:{_RPC_SECRET}?x@rpc.example/v2/key",
            f"https://user:{_RPC_SECRET}#x@rpc.example/v2/key",
            f"https://user:p@{_RPC_SECRET}/x@rpc.example/v2/key",
            f"https://user:{_RPC_SECRET}",
            f"https://rpc.example:{_RPC_SECRET}/v2/",
            f"https://user:{_RPC_SECRET}]",
            f"https://user:{_RPC_SECRET}:123",
            "https://user:!!!",
            "https://user:123456",
            # Too long for int(), which must not make redaction raise.
            "https://user:" + "1" * 5000,
        ):
            with self.subTest(url=url):
                self.assertEqual(redact_url(url), f"https://{REDACTED_LOG_VALUE}")
                self.assertEqual(
                    redact_urls(f"Node {url} failed."),
                    f"Node https://{REDACTED_LOG_VALUE} failed.",
                )

        # Hosts and numeric ports are kept to identify the endpoint.
        for host in (
            "rpc.example:8545",
            "rpc.example:08545",
            "[::1]:8545",
            "[::1]",
            "node-name.provider.example",
        ):
            with self.subTest(host=host):
                self.assertEqual(
                    redact_url(f"https://{host}/v2/key"),
                    f"https://{host}/{REDACTED_LOG_VALUE}",
                )
        for text in (
            "Connecting to (https://rpc.example:8545).",
            "Connecting to [https://rpc.example:8545].",
            "Connecting to https://[::1]:8545.",
        ):
            with self.subTest(text=text):
                self.assertEqual(redact_urls(text), text)

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
            # Punctuation can be the whole path or query after the host.
            "https://rpc.example/!!!": _REDACTED_RPC_URL,
            "https://rpc.example?.:;": _REDACTED_RPC_URL,
            "https://rpc.example/?!!!": _REDACTED_RPC_URL,
            "Node https://rpc.example/!!! is down": (
                f"Node {_REDACTED_RPC_URL} is down"
            ),
            "'https://rpc.example/!!!'": f"'{_REDACTED_RPC_URL}'",
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

    def test_redact_exception_redacts_chain_in_place(self):
        """Messages in an exception chain are redacted; types and attributes are kept."""
        request = requests.Request("POST", _RPC_URL).prepare()
        error = _connection_error(f"/v2/{_RPC_SECRET}")
        error.request = request
        max_retry_error = error.args[0]

        self.assertIs(redact_exception(error), error)

        self.assertNotIn(_RPC_SECRET, _traceback_text(error))
        self.assertIn(f"url: /{REDACTED_LOG_VALUE}", str(error))
        self.assertIs(error.args[0], max_retry_error)
        self.assertIs(error.__cause__, max_retry_error)
        self.assertIs(error.request, request)
        self.assertEqual(redact_exception(error).args, error.args)
        # A worker process passes an exception to its parent by pickling it,
        # and urllib3 rebuilds the message from its url attribute when the
        # exception is unpickled. Simulate that round trip.
        self.assertEqual(max_retry_error.url, f"/{REDACTED_LOG_VALUE}")
        self.assertNotIn(
            _RPC_SECRET, _traceback_text(pickle.loads(pickle.dumps(error)))
        )

    def test_redact_exception_redacts_other_traceback_text(self):
        """Notes, OSError text, and exception groups are redacted too."""
        os_error = ConnectionRefusedError(111, f"Connection refused for {_RPC_URL}")
        file_error = FileNotFoundError(2, "No such file", _RPC_URL, None, _RPC_URL)
        noted_error = requests.ConnectionError("Connection failed")
        noted_error.add_note(f"Request endpoint: {_RPC_URL}")
        error_group = ExceptionGroup("Requests failed", [_connection_error(_RPC_URL)])
        for error in (os_error, file_error, noted_error, error_group):
            with self.subTest(error=type(error).__name__):
                redact_exception(error)

                self.assertNotIn(_RPC_SECRET, _traceback_text(error))
                self.assertNotIn(
                    _RPC_SECRET, _traceback_text(pickle.loads(pickle.dumps(error)))
                )

        self.assertEqual(
            str(os_error), f"[Errno 111] Connection refused for {_REDACTED_RPC_URL}"
        )
        self.assertEqual(
            str(file_error),
            f"[Errno 2] No such file: '{_REDACTED_RPC_URL}' -> '{_REDACTED_RPC_URL}'",
        )
        self.assertIn(
            f"Request endpoint: {_REDACTED_RPC_URL}", _traceback_text(noted_error)
        )

    def test_redact_exception_keeps_exception_group_title(self):
        """By design, an exception group's title keeps its text.

        The title is the group's read-only message attribute, so it cannot be
        redacted in place; the exceptions in the group are redacted. See "URL
        Redaction Limits" in internal/specs/development.md.
        """
        title = f"Requests to {_RPC_URL} failed"
        inner_error = _connection_error(f"/v2/{_RPC_SECRET}")
        error_group = ExceptionGroup(title, [inner_error])

        redact_exception(error_group)

        self.assertEqual(error_group.message, title)
        self.assertNotIn(_RPC_SECRET, _traceback_text(inner_error))

    @patch("requests.Session.post")
    def test_node_request_errors_redact_rpc_url(self, post_mock):
        """Errors from node requests do not show the node URL's credentials."""
        response = requests.Response()
        response.status_code = 429
        response.reason = "Too Many Requests"
        response.url = _RPC_URL
        response.raw = io.BytesIO(b"")
        cases = (
            (
                requests.HTTPError,
                {"return_value": response},
                f"429 Client Error: Too Many Requests for url: {_REDACTED_RPC_URL}",
            ),
            (
                requests.ConnectionError,
                {"side_effect": _connection_error(f"/v2/{_RPC_SECRET}")},
                f"None: Max retries exceeded with url: /{REDACTED_LOG_VALUE} "
                "(Caused by OSError('Connection refused'))",
            ),
        )
        for error_type, post_result, message in cases:
            with self.subTest(error_type=error_type.__name__):
                post_mock.reset_mock(return_value=True, side_effect=True)
                post_mock.configure_mock(**post_result)
                provider = _RedactingHTTPProvider(
                    _RPC_URL, exception_retry_configuration=None
                )

                with self.assertRaises(error_type) as raised:
                    provider.make_request("eth_blockNumber", [])

                self.assertEqual(str(raised.exception), message)
                self.assertNotIn(_RPC_SECRET, _traceback_text(raised.exception))

    @patch("requests.Session.post")
    def test_node_error_responses_redact_urls(self, post_mock):
        """Errors that web3 raises for node responses omit URL credentials."""
        node_text = f"upstream {_RPC_URL} failed"
        # URLs can also appear as object keys, such as in error.data or in a
        # malformed response.
        cases = (
            (
                Web3RPCError,
                {
                    "jsonrpc": "2.0",
                    "id": 0,
                    "error": {
                        "code": -1,
                        "message": node_text,
                        "data": {_RPC_URL: "timeout"},
                    },
                },
            ),
            (
                BadResponseFormat,
                {"jsonrpc": "2.0", "id": 0, "error": node_text, _RPC_URL: "x"},
            ),
        )
        bodies = [(error_type, json.dumps(body).encode()) for error_type, body in cases]
        # web3 raises JSONDecodeError, with the body in its message, for a
        # body that is not JSON.
        bodies.append((json.JSONDecodeError, node_text.encode()))
        for error_type, body in bodies:
            with self.subTest(error_type=error_type.__name__):
                post_mock.return_value = _node_response(body)
                w3 = Web3(
                    _RedactingHTTPProvider(_RPC_URL, exception_retry_configuration=None)
                )

                with self.assertRaises(error_type) as raised:
                    w3.eth.get_block_number()

                error = raised.exception
                self.assertIn(_REDACTED_RPC_URL, str(error))
                self.assertNotIn(_RPC_SECRET, _traceback_text(error))
                self.assertNotIn(_RPC_SECRET, repr(getattr(error, "rpc_response", "")))
                # A worker process passes an exception to its parent by pickling it.
                self.assertNotIn(
                    _RPC_SECRET, _traceback_text(pickle.loads(pickle.dumps(error)))
                )

        # The result is node data and is returned unchanged.
        post_mock.return_value = _node_response(
            json.dumps({"jsonrpc": "2.0", "id": 0, "result": _RPC_URL}).encode()
        )
        provider = _RedactingHTTPProvider(_RPC_URL, exception_retry_configuration=None)
        self.assertEqual(
            provider.make_request("web3_clientVersion", [])["result"], _RPC_URL
        )

    @patch("requests.Session.post")
    def test_node_revert_data_is_kept(self, post_mock):
        """Revert data is kept, and the node's revert message is redacted.

        By design, a reason that web3 decodes from a "Reverted 0x..." payload
        keeps its text: the contract sets it and does not see the node URL.
        See "URL Redaction Limits" in internal/specs/development.md.
        """
        reason = f"failed at {_RPC_URL}"
        revert_data = "0x08c379a0" + encode(["string"], [reason]).hex()
        cases = (
            (revert_data, f"execution reverted: failed at {_REDACTED_RPC_URL}"),
            (f"Reverted {revert_data}", f"execution reverted: {reason}"),
        )
        for data, message in cases:
            with self.subTest(data=data[:10]):
                post_mock.side_effect = _revert_post(
                    f"execution reverted: {reason}", data
                )
                w3 = Web3(
                    _RedactingHTTPProvider(_RPC_URL, exception_retry_configuration=None)
                )

                with self.assertRaises(ContractLogicError) as raised:
                    w3.eth.call({"to": "0x" + "22" * 20, "data": "0x"})

                self.assertEqual(raised.exception.message, message)
                self.assertEqual(raised.exception.data, data)

    def test_redact_urls_filter_redacts_logged_tracebacks(self):
        """Tracebacks logged through a handler with the filter omit URL credentials."""
        plain_stream = io.StringIO()
        filtered_stream = io.StringIO()
        filtered_handler = logging.StreamHandler(filtered_stream)
        filtered_handler.addFilter(RedactUrlsFilter())
        logger = logging.getLogger(f"{__name__}.tracebacks")
        logger.propagate = False
        # The first handler has no filter and caches the traceback text.
        for handler in (logging.StreamHandler(plain_stream), filtered_handler):
            logger.addHandler(handler)
            self.addCleanup(logger.removeHandler, handler)

        try:
            raise _connection_error(f"/v2/{_RPC_SECRET}")
        except requests.ConnectionError:
            logger.exception("Request failed")
        # A record received from another process carries only the text.
        filtered_handler.handle(
            logging.makeLogRecord(
                {"msg": "Remote request failed", "exc_text": f"Error: {_RPC_URL}"}
            )
        )

        # The cached text is unredacted, so the filtered handler must not use it.
        self.assertIn(_RPC_SECRET, plain_stream.getvalue())
        self.assertIn("Traceback", filtered_stream.getvalue())
        self.assertIn(f"url: /{REDACTED_LOG_VALUE}", filtered_stream.getvalue())
        self.assertIn(f"Error: {_REDACTED_RPC_URL}", filtered_stream.getvalue())
        self.assertNotIn(_RPC_SECRET, filtered_stream.getvalue())

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
                # Node request errors are redacted by the provider.
                self.assertIsInstance(
                    web3_mock.call_args.args[0], _RedactingHTTPProvider
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
                    with self.assertRaises(requests.HTTPError) as raised:
                        service.get_commitment_service_data()

        self.assertIs(raised.exception.response, response)
        self.assertNotIn(secret, _traceback_text(raised.exception))
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
                        with self.assertRaises(requests.ConnectionError) as raised:
                            service.get_commitment_service_data()

                self.assertNotIn(secret, _traceback_text(raised.exception))
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

    @patch("vbase.core.web3_http_commitment_service.time.sleep")
    @patch("vbase.core.web3_http_commitment_service._W3_CONNECTION_MAX_RETRIES", 1)
    def test_web3_malformed_node_url_is_redacted(self, _sleep_mock):
        """A malformed node URL fails to connect without showing its credentials."""
        # requests shows these URLs as given in its errors, which web3's
        # connection check handles. The connection error and its log show the
        # configured URL redacted as one value.
        for node_rpc_url in (
            f"rpc.example/v2/{_RPC_SECRET}",
            f"user:{_RPC_SECRET}@rpc.example/",
            f"https:\t//rpc.example/v2/{_RPC_SECRET}",
            f"https://rpc.example:\t443/v2/{_RPC_SECRET}",
            f"https://user:{_RPC_SECRET}/x@rpc.example/v2/key",
            f"https://user:{_RPC_SECRET}",
        ):
            with self.subTest(node_rpc_url=node_rpc_url):
                with self.assertLogs(
                    "vbase.core.web3_http_commitment_service", level="ERROR"
                ) as captured:
                    with self.assertRaises(ConnectionError) as raised:
                        Web3HTTPCommitmentService(
                            node_rpc_url=node_rpc_url,
                            commitment_service_address="0x" + "33" * 20,
                        )

                self.assertIn(REDACTED_LOG_VALUE, str(raised.exception))
                self.assertNotIn(_RPC_SECRET, _traceback_text(raised.exception))
                self.assertNotIn(_RPC_SECRET, "\n".join(captured.output))

    def test_forwarder_rejects_malformed_url(self):
        """A forwarder URL that requests could show unredacted is rejected."""
        # urlsplit removes tabs and newlines before parsing, but requests does
        # not, so URLs with them in the scheme, host, or port fail in requests
        # with the URL in a form that redaction does not recognize.
        for forwarder_url in (
            f"forwarder.example/v1/{_RPC_SECRET}/",
            f"user:{_RPC_SECRET}@forwarder.example/",
            f"http:/forwarder.example/{_RPC_SECRET}/",
            f"ftp://forwarder.example/{_RPC_SECRET}/",
            "",
            f"https:\t//forwarder.example/v1/{_RPC_SECRET}/",
            f"https:\r//forwarder.example/v1/{_RPC_SECRET}/",
            f"https:\n//forwarder.example/v1/{_RPC_SECRET}/",
            f"ht\ttps://forwarder.example/v1/{_RPC_SECRET}/",
            f"https://forwarder.example:\t443/v1/{_RPC_SECRET}/",
            f"https://forwarder.example/v1/{_RPC_SECRET}/ ",
            f"https://forwarder.example/v1/\x00{_RPC_SECRET}/",
        ):
            with self.subTest(forwarder_url=forwarder_url):
                with self.assertRaises(ValueError) as raised:
                    ForwarderCommitmentService(
                        forwarder_url=forwarder_url, api_key="test-api-key"
                    )

                self.assertNotIn(_RPC_SECRET, _traceback_text(raised.exception))

        # requests removes leading whitespace, so it is accepted.
        for forwarder_url in (
            "https://forwarder.example/",
            "http://127.0.0.1:3000/forwarder-test/",
            " \thttps://forwarder.example/",
        ):
            with self.subTest(forwarder_url=forwarder_url):
                service = ForwarderCommitmentService(
                    forwarder_url=forwarder_url, api_key="test-api-key"
                )
                self.assertEqual(service.forwarder_url, forwarder_url)


if __name__ == "__main__":
    unittest.main()
