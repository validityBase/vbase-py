"""Offline tests of how the forwarder test scripts handle credentials."""

# pylint: disable=protected-access

import importlib.util
import io
import os
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import ModuleType
from unittest import mock

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_script(relative_path: str) -> ModuleType:
    """Load a script that is not part of an importable package.

    :param relative_path: The script's path relative to the repository root.
    :return: The loaded module.
    """
    path = _REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_CHECK_FORWARDER = _load_script("vbase/tests/scripts/check_forwarder.py")
_PUB_DEV_RUNNER = _load_script(".github/scripts/run_tests_forwarder_pub_dev.py")


class TestCheckForwarderOrigin(unittest.TestCase):
    """Readiness messages must not show URL credentials."""

    def test_keeps_only_scheme_host_and_port(self) -> None:
        """Paths, queries, and userinfo are removed."""
        cases = {
            "http://127.0.0.1:8081/forwarder-test/": "http://127.0.0.1:8081",
            "https://rpc.example/v2/FAKE-KEY-0123456789": "https://rpc.example",
            "https://rpc.example/rpc?apikey=FAKE-KEY": "https://rpc.example",
            "https://user:FAKE-PASSWORD@rpc.example:8545/": "https://rpc.example:8545",
        }
        for url, expected in cases.items():
            with self.subTest(url=url):
                self.assertEqual(_CHECK_FORWARDER._origin(url), expected)

    def test_hides_host_part_that_may_hold_credentials(self) -> None:
        """An "@" after the host part means credentials ended it early."""
        for url in (
            "https://FAKE-TOKEN/segment@rpc.example/",
            "https://FAKE-TOKEN?segment@rpc.example/",
            "https://FAKE-TOKEN#segment@rpc.example/",
        ):
            with self.subTest(url=url):
                self.assertEqual(_CHECK_FORWARDER._origin(url), "https://[REDACTED]")

    def test_hides_invalid_port(self) -> None:
        """A port that is not a number can be a password without its host."""
        self.assertEqual(
            _CHECK_FORWARDER._origin("https://user:FAKE-PASSWORD/rpc.example/"),
            "<unparsable URL>",
        )


class TestCheckForwarderNode(unittest.TestCase):
    """The node check reports a bad contract address as a forwarder problem."""

    def test_invalid_address_fails_before_contacting_node(self) -> None:
        """A malformed address is not reported as a node failure."""
        with mock.patch.object(_CHECK_FORWARDER.Web3, "HTTPProvider") as provider:
            with self.assertRaisesRegex(
                SystemExit, "invalid commitment service address '0x1234'"
            ):
                _CHECK_FORWARDER._check_node("http://127.0.0.1:8545/", "0x1234")
        provider.assert_not_called()


class TestPublicDevRunnerMasks(unittest.TestCase):
    """The public-dev runner masks the indexing URL's credential-bearing parts."""

    @staticmethod
    def _masks(environment: dict[str, str]) -> list[str]:
        """Return the workflow commands printed for an environment."""
        output = io.StringIO()
        with mock.patch.dict(os.environ, environment, clear=True):
            with redirect_stdout(output):
                _PUB_DEV_RUNNER._mask_indexing_rpc_url_parts()
        return output.getvalue().splitlines()

    def test_masks_path_parts_of_trimmed_url(self) -> None:
        """A connection error that shows only the path cannot show the key."""
        masks = self._masks(
            {
                "GITHUB_ACTIONS": "true",
                "VBASE_TEST_INDEXING_NODE_RPC_URL": (
                    " https://rpc.example/v2/FAKE-KEY-0123456789\n"
                ),
            }
        )
        self.assertEqual(
            masks,
            [
                "::add-mask::FAKE-KEY-0123456789",
                "::add-mask::https://rpc.example/v2/FAKE-KEY-0123456789",
                "::add-mask::v2/FAKE-KEY-0123456789",
            ],
        )

    def test_masks_query_and_password(self) -> None:
        """Credentials in the query or userinfo are masked; short parts are not."""
        masks = self._masks(
            {
                "GITHUB_ACTIONS": "true",
                "VBASE_TEST_INDEXING_NODE_RPC_URL": (
                    "https://user:FAKE-PASSWORD@rpc.example/rpc?apikey=FAKE-KEY"
                ),
            }
        )
        self.assertIn("::add-mask::FAKE-PASSWORD", masks)
        self.assertIn("::add-mask::apikey=FAKE-KEY", masks)
        self.assertNotIn("::add-mask::user", masks)
        self.assertNotIn("::add-mask::rpc", masks)

    def test_escapes_workflow_command_characters(self) -> None:
        """A percent sign in the URL survives workflow command decoding."""
        masks = self._masks(
            {
                "GITHUB_ACTIONS": "true",
                "VBASE_TEST_INDEXING_NODE_RPC_URL": "https://rpc.example/v2/FAKE%2FKEY",
            }
        )
        self.assertIn("::add-mask::FAKE%252FKEY", masks)

    def test_masks_nothing_outside_github_actions(self) -> None:
        """Local runs print no workflow commands."""
        masks = self._masks(
            {"VBASE_TEST_INDEXING_NODE_RPC_URL": "https://rpc.example/v2/FAKE-KEY"}
        )
        self.assertEqual(masks, [])


if __name__ == "__main__":
    unittest.main()
