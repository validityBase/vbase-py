"""Check that the configured forwarder and its node are ready for the tests.

The localhost forwarder test runner calls this before the tests, so that a
missing service or a wrong API key fails once with a clear message instead of
in every test. Messages show only the scheme, host, and port of URLs.
"""

import os
from urllib.parse import urlsplit

import requests
from web3 import Web3

_TIMEOUT = 10.0


def _origin(url: str) -> str:
    """Return the scheme, host, and port of a URL, without credentials or path.

    :param url: The URL.
    :return: The origin, or a placeholder if the URL cannot be parsed.
    """
    try:
        parts = urlsplit(url)
        port = f":{parts.port}" if parts.port else ""
    except ValueError:
        return "<unparsable URL>"
    return f"{parts.scheme}://{parts.hostname}{port}"


def _get_commitment_service_data(forwarder_url: str, api_key: str) -> dict:
    """Return the forwarder's commitment service data.

    :param forwarder_url: The forwarder URL, ending with a slash.
    :param api_key: The forwarder API key.
    :return: The data with node_rpc_url and commitment_service_address.
    :raises SystemExit: If the forwarder does not return the data.
    """
    origin = _origin(forwarder_url)
    try:
        response = requests.get(
            forwarder_url + "commitment-service-data",
            headers={"X-API-KEY": api_key},
            timeout=_TIMEOUT,
        )
    except requests.RequestException as err:
        raise SystemExit(
            f"Forwarder check failed: cannot connect to {origin} "
            f"({type(err).__name__}). Start the forwarder service or check "
            "the forwarder port."
        ) from err
    if response.status_code == 401:
        raise SystemExit(
            "Forwarder check failed: the forwarder rejected VBASE_API_KEY. "
            "Use an API key that the forwarder accepts."
        )
    try:
        data = response.json().get("data")
    except (ValueError, AttributeError) as err:
        raise SystemExit(
            f"Forwarder check failed: {origin} returned HTTP "
            f"{response.status_code} without forwarder JSON. Another service "
            "may be using the port."
        ) from err
    if (
        response.status_code != 200
        or not isinstance(data, dict)
        or not data.get("commitment_service_address")
    ):
        raise SystemExit(
            f"Forwarder check failed: {origin} returned HTTP "
            f"{response.status_code} without commitment service data."
        )
    return data


def _check_node(node_rpc_url: str, commitment_service_address: str) -> int:
    """Check that the node is reachable and has the commitment service contract.

    :param node_rpc_url: The explicitly configured indexing node RPC URL.
    :param commitment_service_address: The contract address.
    :return: The node's chain ID.
    :raises SystemExit: If the node is unreachable or has no contract.
    """
    origin = _origin(node_rpc_url)
    w3 = Web3(Web3.HTTPProvider(node_rpc_url, request_kwargs={"timeout": _TIMEOUT}))
    try:
        chain_id = w3.eth.chain_id
        code = w3.eth.get_code(Web3.to_checksum_address(commitment_service_address))
    # Node errors can include the node URL, so report only the error type.
    except Exception as err:  # pylint: disable=broad-exception-caught
        raise SystemExit(
            f"Forwarder check failed: cannot query the node at {origin} "
            f"({type(err).__name__})."
        ) from err
    if not code:
        raise SystemExit(
            "Forwarder check failed: the node has no contract at "
            f"{commitment_service_address}. Deploy the commitment service "
            "contracts."
        )
    return chain_id


def main() -> None:
    """Check the forwarder configured in the environment."""
    forwarder_url = os.environ.get("VBASE_FORWARDER_URL")
    api_key = os.environ.get("VBASE_API_KEY")
    node_rpc_url = os.environ.get("VBASE_TEST_INDEXING_NODE_RPC_URL")
    if not forwarder_url or not api_key or not node_rpc_url:
        raise SystemExit(
            "Forwarder check failed: set VBASE_FORWARDER_URL, VBASE_API_KEY, "
            "and VBASE_TEST_INDEXING_NODE_RPC_URL."
        )
    try:
        _ = urlsplit(forwarder_url).port
    except ValueError as err:
        raise SystemExit(
            "Forwarder check failed: VBASE_FORWARDER_URL has an invalid port."
        ) from err
    data = _get_commitment_service_data(forwarder_url, api_key)
    chain_id = _check_node(node_rpc_url, data["commitment_service_address"])
    print(
        f"Forwarder ready: {_origin(forwarder_url)}, chain {chain_id}, "
        f"contract {data['commitment_service_address']}."
    )


if __name__ == "__main__":
    main()
