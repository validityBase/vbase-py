"""Wait until the localhost test chain has the commitment service contract.

The localhost test runners call this before the tests. The localhost image
deploys the contract after its node starts, so tests that start at once can
fail. This waits up to a deadline, then stops with one clear message instead
of failing in every test.
"""

import os
import time

from web3 import Web3

_WAIT_SECONDS = 60.0
_REQUEST_TIMEOUT_SECONDS = 2.0


def main() -> None:
    """Wait for code at the configured commitment service address."""
    node_rpc_url = os.environ.get("VBASE_COMMITMENT_SERVICE_NODE_RPC_URL")
    address = os.environ.get("VBASE_COMMITMENT_SERVICE_ADDRESS")
    if not node_rpc_url or not address:
        raise SystemExit(
            "Set VBASE_COMMITMENT_SERVICE_NODE_RPC_URL and "
            "VBASE_COMMITMENT_SERVICE_ADDRESS, as config/.env.localhost does."
        )
    address = Web3.to_checksum_address(address)
    # Each request fails fast; the loop below does the retrying.
    w3 = Web3(
        Web3.HTTPProvider(
            node_rpc_url,
            request_kwargs={"timeout": _REQUEST_TIMEOUT_SECONDS},
            exception_retry_configuration=None,
        )
    )
    deadline = time.monotonic() + _WAIT_SECONDS
    while True:
        try:
            if w3.eth.get_code(address):
                return
            reason = "the node has no code at this address"
        # Node errors can include the node URL, so report only the error type.
        except Exception as err:  # pylint: disable=broad-exception-caught
            reason = f"cannot query the node ({type(err).__name__})"
        if time.monotonic() >= deadline:
            raise SystemExit(
                f"No commitment contract at {address} after "
                f"{_WAIT_SECONDS:.0f} seconds: {reason}."
            )
        time.sleep(1)


if __name__ == "__main__":
    main()
