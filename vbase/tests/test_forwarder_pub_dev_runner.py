"""Test the public dev forwarder test runner."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


class TestForwarderPubDevRunner(unittest.TestCase):
    """Test the shell runner without calling the public dev service."""

    def test_runner_replaces_configured_signer_with_ephemeral_key(self):
        """Use the generated signer for every public dev test command."""
        repository_root = Path(__file__).resolve().parents[2]
        runner = repository_root / "vbase/tests/scripts/run_tests_forwarder_pub_dev.sh"

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            fake_bin = temporary_path / "bin"
            fake_bin.mkdir()
            call_log = temporary_path / "calls.log"
            fake_python = fake_bin / "python3"
            fake_python.write_text(
                """#!/bin/bash
if [[ "$1" == "-c" ]]; then
    printf '%s\\n' '0x1111111111111111111111111111111111111111111111111111111111111111'
    exit 0
fi
printf '%s|%s\\n' "${VBASE_COMMITMENT_SERVICE_PRIVATE_KEY:-}" "$*" >> "$CALL_LOG"
""",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)

            environment = os.environ.copy()
            environment["CALL_LOG"] = str(call_log)
            environment["PATH"] = f"{fake_bin}:{environment['PATH']}"

            subprocess.run(
                ["bash", str(runner)],
                cwd=repository_root,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )

            calls = call_log.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(calls), 2)
        self.assertTrue(
            all(
                call.startswith(
                    "0x1111111111111111111111111111111111111111111111111111111111111111|"
                )
                for call in calls
            ),
            "The public dev runner did not replace the configured signer key",
        )
        self.assertEqual(
            [call.split("|", maxsplit=1)[1] for call in calls],
            [
                "-m unittest vbase.tests.test_vbase_client",
                "-m unittest vbase.tests.test_indexing_service",
            ],
        )
