"""Test crypto utilities
"""

import hashlib
import unittest

from web3 import Web3

from vbase.utils.crypto_utils import (
    convert_typed_values_to_bytes,
    hash_typed_values,
    solidity_hash_typed_values,
)

_STR_TEST = "Hello, world!"
_INT_TEST1 = 42
_INT_TEST2 = 43


def sha3_256_hash_bytes(data: bytes) -> str:
    """Compute a SHA3-256 hash of a byte array."""
    hash_obj = hashlib.sha3_256()
    hash_obj.update(data)
    sha3_hash = "0x" + hash_obj.digest().hex()
    return sha3_hash


class TestCryptoUtils(unittest.TestCase):
    """Test vBase crypto utilities"""

    def test_str_hash(self):
        """Test hash of a string against reference values."""
        # Verify (keccak-256) solidity_hash_typed_values for a string.
        sol_hash = solidity_hash_typed_values(["string"], [_STR_TEST])
        keccak_hash = Web3.keccak(text=_STR_TEST).hex()
        self.assertEqual(sol_hash, keccak_hash)
        # Verify the same hash using the marshalling function.
        keccak_hash = Web3.keccak(
            convert_typed_values_to_bytes(["string"], [_STR_TEST])
        ).hex()
        self.assertEqual(sol_hash, keccak_hash)
        # Verify (sha3-256) hash_typed_values for a string.
        vbase_hash = hash_typed_values(["string"], [_STR_TEST])
        sha3_hash = sha3_256_hash_bytes(_STR_TEST.encode("utf-8"))
        self.assertEqual(vbase_hash, sha3_hash)

    def test_int_hash(self):
        """Test hash of an integer against reference values."""
        sol_hash = solidity_hash_typed_values(["uint256"], [_INT_TEST1])
        keccak_hash = Web3.keccak(_INT_TEST1.to_bytes(32, byteorder="big")).hex()
        self.assertEqual(sol_hash, keccak_hash)
        keccak_hash = Web3.keccak(
            convert_typed_values_to_bytes(["uint256"], [_INT_TEST1])
        ).hex()
        self.assertEqual(sol_hash, keccak_hash)
        vbase_hash = hash_typed_values(["uint256"], [_INT_TEST1])
        sha3_hash = sha3_256_hash_bytes(_INT_TEST1.to_bytes(32, byteorder="big"))
        self.assertEqual(vbase_hash, sha3_hash)

    def test_2ints_hash(self):
        """Test hash of two integers against reference values."""
        sol_hash = solidity_hash_typed_values(
            ["uint256", "uint256"], [_INT_TEST1, _INT_TEST2]
        )
        keccak_hash = Web3.keccak(
            _INT_TEST1.to_bytes(32, byteorder="big")
            + _INT_TEST2.to_bytes(32, byteorder="big")
        ).hex()
        self.assertEqual(sol_hash, keccak_hash)
        keccak_hash = Web3.keccak(
            convert_typed_values_to_bytes(
                ["uint256", "uint256"], [_INT_TEST1, _INT_TEST2]
            )
        ).hex()
        self.assertEqual(sol_hash, keccak_hash)
        vbase_hash = hash_typed_values(["uint256", "uint256"], [_INT_TEST1, _INT_TEST2])
        sha3_hash = sha3_256_hash_bytes(
            _INT_TEST1.to_bytes(32, byteorder="big")
            + _INT_TEST2.to_bytes(32, byteorder="big")
        )
        self.assertEqual(vbase_hash, sha3_hash)

    def test_str_int_hash(self):
        """Test hash of a string followed by an integer against reference values."""
        sol_hash = solidity_hash_typed_values(
            ["string", "uint256"], [_STR_TEST, _INT_TEST2]
        )
        keccak_hash = Web3.keccak(
            _STR_TEST.encode("utf-8") + _INT_TEST2.to_bytes(32, byteorder="big")
        ).hex()
        self.assertEqual(sol_hash, keccak_hash)
        keccak_hash = Web3.keccak(
            convert_typed_values_to_bytes(
                ["string", "uint256"], [_STR_TEST, _INT_TEST2]
            )
        ).hex()
        self.assertEqual(sol_hash, keccak_hash)
        vbase_hash = hash_typed_values(["string", "uint256"], [_STR_TEST, _INT_TEST2])
        sha3_hash = sha3_256_hash_bytes(
            _STR_TEST.encode("utf-8") + _INT_TEST2.to_bytes(32, byteorder="big")
        )
        self.assertEqual(vbase_hash, sha3_hash)


if __name__ == "__main__":
    unittest.main()
