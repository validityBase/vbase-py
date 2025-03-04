"""
Common cryptographic utility functions
"""

import hashlib
from typing import Any, List, Union

from eth_typing import HexStr
from eth_utils import add_0x_prefix, remove_0x_prefix
from web3 import Web3
from web3._utils.encoding import hex_encode_abi_type

# Field prime P used in ZK proofs
# Field element are positive integer values in [0, p - 1].
# Field math overflows at p.
# We represent negative value x as p - x.
# Define p for the ALT_BN128 curve supported by Ethereum.
FIELD_P = 21888242871839275222246405745257275088548364400416034343698204186575808495617

# Fixed point base used to represent fractions
# ZK proofs operate on uints, so fractions must be converted to uints.
DECIMALS = 9
DECIMALS_BASE = int(1e9)


def solidity_hash_typed_values(abi_types: List[str], values: List[Any]) -> str:
    """
    Calculates a keccak256 hash exactly as Solidity does.

    :param abi_types: A list of Solidity ABI types.
    :param values: A list of values to hash.
    :return: The resulting hash.
    """
    # The following line creates overactive warning
    # because of difficulties with a decorated declaration:
    # E1120: No value for argument 'values' in unbound method call (no-value-for-parameter)
    # pylint: disable=E1120
    return Web3.solidity_keccak(abi_types, values).hex()


def convert_typed_values_to_bytes(abi_types: List[str], values: List[Any]) -> bytes:
    """
    Marshalls ABI types as Solidity does.
    Based on the marshalling in solidity_keccak.
    We use this function to factor out Web3/Solidity marshalling
    and unit-test it to ensure compatibility with Solidity.
    Actual hashing is done in the hash_typed_values function
    where we use the standard Python hashlib library's sha3-256.

    :param abi_types: A list of Solidity ABI types.
    :param values: A list of values to hash.
    :return: The resulting bytes.
    """
    if len(abi_types) != len(values):
        raise ValueError(
            "Length mismatch between provided abi types and values.  Got "
            f"{len(abi_types)} types and {len(values)} values."
        )

    normalized_values = Web3.normalize_values(None, abi_types, values)

    hex_string = HexStr(
        "".join(
            remove_0x_prefix(hex_encode_abi_type(abi_type, value))
            for abi_type, value in zip(abi_types, normalized_values)
        )
    )
    return bytes.fromhex(hex_string)


def hash_typed_values(abi_types: List[str], values: List[Any]) -> str:
    """
    Calculates a sha3-256 hash on ABI types marshalled as Solidity does.
    Based on the marshalling in solidity_keccak.

    :param abi_types: A list of Solidity ABI types.
    :param values: A list of values to hash.
    :return: The resulting hash.
    """
    data_bytes = convert_typed_values_to_bytes(abi_types, values)
    hash_obj = hashlib.sha3_256()
    hash_obj.update(data_bytes)
    return add_0x_prefix(str(hash_obj.hexdigest()))


def bytes_to_hex_str(byte_arr: bytes) -> str:
    """
    Convert a byte array to a hex string.

    :param byte_arr: The byte array to convert.
    :return: The resulting hex string.
    """
    return "0x" + byte_arr.hex()


def bytes_to_hex_str_auto(byte_arr: Union[bytes, str]) -> str:
    """
    Convert a byte array to a hex string
    with intelligent conversion of bytes and string representations.
    Some APIs may return byte array as bytes, HexBytes, or a string,
    depending on the nodes and paths they use.

    :param byte_arr: The byte array to convert.
    :return: The resulting hex string.
    """
    if isinstance(byte_arr, bytes):
        hex_str = byte_arr.hex()
    else:
        hex_str = str(byte_arr)
    if hex_str.startswith("0x"):
        return hex_str
    return "0x" + hex_str


def hex_str_to_bytes(hex_str: str) -> bytes:
    """
    Convert a hex string to a byte array.

    :param hex_str: The hex string to convert.
    :return: The resulting byte array.
    """
    return bytes.fromhex(hex_str[2:])


def hex_str_to_int(hex_str: str) -> int:
    """
    Convert a hex string to an integer.

    :param hex_str: The hex string to convert.
    :return: The resulting integer.
    """
    return int(hex_str, 16)


def add_int_uint256(n1: int, n2_hex_str: str) -> int:
    """
    Add int and uint256 with overflow and wrap-around.
    This replicates the following sol code:
    unchecked {
        userSetObjectCidSums[userSetCid] += uint256(objectCid);
    }

    :param n1: The first integer.
    :param n2_hex_str: The second integer, passed as a hex string.
    :returns: The resulting uint256 sum.
    """
    return (n1 + int(n2_hex_str, 16)) % (2**256)


def add_uint256_uint256(n1_hex_str: str, n2_hex_str: str) -> str:
    """
    Add int and uint256 with overflow and wrap-around.
    This replicates the following sol code:
    unchecked {
        userSetObjectCidSums[userSetCid] += uint256(objectCid);
    }

    :param n1_hex_str: The first integer, passed as a hex string.
    :param n2_hex_str: The second integer, passed as a hex string.
    :returns: The resulting uint256 sum,
    """
    return hex((int(n1_hex_str, 16) + int(n2_hex_str, 16)) % (2**256))


def string_to_u64_id(s: str) -> int:
    """
    Convert a string to an u64 id used in commitments.

    :param s: The string to convert.
    :returns: The u64 integer used in commitments.
    """
    return int(hash_typed_values(["string"], [s]), 16) % (2**64)


def float_to_field(x: float) -> int:
    """
    Convert a float to a field int used in commitments.

    :param x: The float to convert.
    :return: The field integer used in commitments.
    """
    # It is important to round the resulting integer to avoid trailing values.
    # This breaks ZKPs.
    x_fixed = round(int(x * DECIMALS_BASE), -6)
    # Check for overflows
    assert x_fixed < FIELD_P
    # Convert to negative value, if necessary.
    if x_fixed < 0:
        x_fixed += FIELD_P
    return x_fixed


def field_to_float(x: int) -> float:
    """
    Convert a field integer used in commitments to a float.

    :param x: The field integer used in commitments.
    :return: The resulting float.
    """
    assert x < FIELD_P
    if x > FIELD_P / 2:
        # Handle negative values.
        return -(FIELD_P - x) / DECIMALS_BASE
    return x / DECIMALS_BASE
