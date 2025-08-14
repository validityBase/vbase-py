"""Tests of the vbase_dataset module
"""

import logging
import time
import unittest
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import imageio
import numpy as np
import pandas as pd

from vbase.core.vbase_client import VBaseClient
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.vbase_dataset import VBaseDataset
from vbase.core.vbase_object import (
    VBaseBytesObject,
    VBaseFloatObject,
    VBaseIntObject,
    VBaseJsonObject,
    VBasePortfolioObject,
    VBasePrivateFloatObject,
    VBasePrivateIntObject,
    VBaseStringObject,
)
from vbase.tests.test_crypto_utils import sha3_256_hash_bytes
from vbase.tests.utils import (
    create_dataset_worker,
    dataset_add_record_checks,
    dataset_from_json_checks,
)
from vbase.utils.crypto_utils import add_uint256_uint256
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


def create_png_bytes_from_array(array: np.ndarray) -> bytes:
    """Create a PNG image from a NumPy array and return it as bytes."""
    buf = BytesIO()
    imageio.imwrite(buf, array, format="png")
    return buf.getvalue()


def create_test_image(save_file: bool = False) -> bytes:
    """Create a simple 200x200 black PNG image in memory.

    Args:
        save_file (bool): If True, saves the image as 'image_sample.png' next to the script.

    Returns:
        bytes: The image content in bytes.

    """
    img = np.zeros((200, 200, 3), dtype=np.uint8)
    image_bytes = create_png_bytes_from_array(img)
    # Optionally save to disk
    if save_file:
        image_path = Path(__file__).parent / "image_sample.png"
        with open(image_path, "wb") as f:
            f.write(image_bytes)

    return image_bytes


class TestVBaseDataset(unittest.TestCase):
    """Test base vBase dataset functionality."""

    def setUp(self):
        """Set up the tests."""
        self.vbc = VBaseClientTest.create_instance_from_env()

    @staticmethod
    def _add_record_worker(
        vbc: VBaseClient, dsw: VBaseDataset, record_data: any, t_prev: pd.Timestamp
    ):
        cl = dsw.add_record(record_data)
        dataset_add_record_checks(vbc, dsw, cl, t_prev)
        return cl, cl["timestamp"]

    def test_1d_int_ts_wr(self):
        """Test a simple int 1-dimension timeseries write and read."""
        dsw = create_dataset_worker(self.vbc, VBaseIntObject)
        # Record 4 commitments over 4 blocks.
        # We should get a new block with each commitment due to automine.
        # Sleep briefly to make sure timestamps don't collide.
        # We can test a max 4 commitments
        # since hardhat localhost node has issues handling more than 8 events
        # emitted in a single transaction.
        t_prev = 0
        for i in range(1, 5):
            # Commit an integer.
            cl, t_prev = self._add_record_worker(self.vbc, dsw, i, t_prev)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_1d_int_ts_batch_wr(self):
        """Test a simple int 1-dimension timeseries batch write and read."""
        dsw = create_dataset_worker(self.vbc, VBaseIntObject)
        record_data_list = list(range(1, 5))
        cls = dsw.add_records_batch(record_data_list)
        for i, record_data in enumerate(record_data_list):
            assert self.vbc.verify_user_object(
                dsw.owner,
                dsw.record_type.get_cid_for_data(record_data),
                cls[0]["timestamp"],
            )
            assert self.vbc.verify_user_object(
                dsw.owner,
                dsw.record_type.get_cid_for_data(record_data),
                cls[i]["timestamp"],
            )
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_pri_1d_int_ts_batch_wr(self):
        """Test a private int 1-dimension timeseries batch write and read."""
        dsw = create_dataset_worker(self.vbc, VBasePrivateIntObject)
        record_data_list = [(i, str(i)) for i in range(1, 5)]
        cls = dsw.add_records_batch(record_data_list)
        for i, record in enumerate(record_data_list):
            assert self.vbc.verify_user_object(
                dsw.owner,
                dsw.record_type.get_cid_for_data(record),
                cls[0]["timestamp"],
            )
            assert self.vbc.verify_user_object(
                dsw.owner,
                dsw.record_type.get_cid_for_data(record),
                cls[i]["timestamp"],
            )
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_1d_float_ts_wr(self):
        """Test a simple float 1-dimension timeseries write and read."""
        dsw = create_dataset_worker(self.vbc, VBaseFloatObject)
        t_prev = self.vbc.commitment_service.convert_timestamp_chain_to_str(0)
        for i in range(1, 5):
            # Commit a float.
            cl, t_prev = self._add_record_worker(self.vbc, dsw, float(i) / 10, t_prev)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_pri_1d_float_ts_wr(self):
        """Test a private float 1-dimension timeseries write and read."""
        dsw = create_dataset_worker(self.vbc, VBasePrivateFloatObject)
        # Use an internal function to quickly build timestamps.
        # noinspection PyUnresolvedReferences
        t_prev = self.vbc.commitment_service.convert_timestamp_chain_to_str(0)
        for i in range(1, 5):
            # Commit a float.
            cl, t_prev = self._add_record_worker(
                self.vbc, dsw, [float(i) / 10, str(i)], t_prev
            )
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_2d_json_ts_wr(self):
        """Test 2-dimension timeseries of Json records write and read."""
        dsw = create_dataset_worker(self.vbc, VBaseJsonObject)
        t_prev = 0
        for i in range(1, 5):
            # Commit a JSON object.
            data = pd.Series(
                [0.2, 0.3 + i / 100, 0.5 - i / 100], index=["AAA", "BBB", "CCC"]
            ).to_json()
            cl, t_prev = self._add_record_worker(self.vbc, dsw, data, t_prev)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_port_wr(self):
        """Test timeseries of portfolio records write and read."""
        dsw = create_dataset_worker(self.vbc, VBasePortfolioObject)
        t_prev = 0
        for i in range(1, 5):
            # Commit a JSON object.
            data = dict(zip(["AAA", "BBB", "CCC"], [0.2, 0.3 + i / 100, 0.5 - i / 100]))
            cl, t_prev = self._add_record_worker(self.vbc, dsw, data, t_prev)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_port_pub_ret(self):
        """Test 1-day returns of a portfolio."""
        # Create the portfolio dataset.
        dsw_port = create_dataset_worker(self.vbc, VBasePortfolioObject, "TestPort")
        self.vbc.clear_named_set_objects("TestPort")
        # Record a portfolio commitment.
        cl = dsw_port.add_record({"AAA": 0.90, "BBB": 0.10})
        assert self.vbc.verify_user_object(
            dsw_port.owner, cl["objectCid"], cl["timestamp"]
        )
        assert self.vbc.verify_user_set_objects(
            dsw_port.owner, dsw_port.cid, str(hex(dsw_port.object_cid_sum))
        )

        # Create the return dataset.
        dsw_rets = VBaseDataset(
            self.vbc, name="TestRets", record_type=VBasePortfolioObject
        )
        self.vbc.clear_named_set_objects("TestRets")
        # Record a return commitment.
        cl = dsw_rets.add_record({"AAA": 0.10, "BBB": 0.20})
        assert self.vbc.verify_user_object(
            dsw_rets.owner, cl["objectCid"], cl["timestamp"]
        )
        assert self.vbc.verify_user_set_objects(
            dsw_rets.owner, dsw_rets.cid, str(hex(dsw_rets.object_cid_sum))
        )

        assert self.vbc.verify_user_sets(
            dsw_port.owner, add_uint256_uint256(dsw_port.cid, dsw_rets.cid)
        )

    def test_str_ts_wr(self):
        """Test timeseries of string records write and read."""
        dsw = create_dataset_worker(self.vbc, VBaseStringObject)
        t_prev = 0
        for i in range(1, 5):
            # Commit a string object.
            data = f"String{i}"
            cl, t_prev = self._add_record_worker(self.vbc, dsw, data, t_prev)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_large_str_ts_wr(self):
        """Test timeseries of large string records writes and reads."""
        dsw = create_dataset_worker(self.vbc, VBaseStringObject)
        t_prev = 0
        for i in range(1, 5):
            # Commit a string object.
            data = f"String{i}" * int(1e6)
            start_time = time.perf_counter()
            cl, t_prev = self._add_record_worker(self.vbc, dsw, data, t_prev)
            total_time = time.perf_counter() - start_time
            _LOG.info("add_record() took %.6f seconds", total_time)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        start_time = time.perf_counter()
        dataset_from_json_checks(self.vbc, dsw, False)
        total_time = time.perf_counter() - start_time
        _LOG.info("ds_from_json_checks() took %.6f seconds", total_time)

    def test_try_restore_timestamps_from_index_success(self):
        """Test try_restore_timestamps_from_index success case."""
        dsw = create_dataset_worker(self.vbc, VBaseIntObject)
        # Record 4 commitments over 4 blocks.
        t_prev = 0
        for i in range(1, 5):
            cl, t_prev = self._add_record_worker(self.vbc, dsw, i, t_prev)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)
        # Mess up the timestamps.
        dsw.timestamps = [
            str(datetime.fromisoformat(t) + timedelta(seconds=1))
            for t in dsw.timestamps
        ]
        success, l_log = dsw.verify_commitments()
        assert not success
        assert len(l_log) == 4
        assert l_log[0].startswith("Invalid record: Failed object verification")
        # Fix the timestamps.
        success, l_log = dsw.try_restore_timestamps_from_index()
        assert success
        success, l_log = dsw.verify_commitments()
        assert success

    def test_try_restore_timestamps_from_index_bad_record(self):
        """Test try_restore_timestamps_from_index success case."""
        dsw = create_dataset_worker(self.vbc, VBaseIntObject)
        # Record 4 commitments over 4 blocks.
        t_prev = 0
        for i in range(1, 5):
            cl, t_prev = self._add_record_worker(self.vbc, dsw, i, t_prev)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)
        # Mess up the timestamps by adding a bogus record.
        dsw.records.append(VBaseIntObject(42))
        dsw.timestamps.append(datetime.now())
        success, l_log = dsw.verify_commitments()
        assert not success
        assert len(l_log) == 2
        assert l_log[0].startswith("Invalid record: Failed object verification")
        assert l_log[1].startswith("Invalid records: Failed object set verification")
        # Try to fix the timestamps.
        success, l_log = dsw.try_restore_timestamps_from_index()
        assert not success
        assert l_log[0].startswith(
            "Invalid record: Failed to find timestamp for object"
        )

    def test_bytes_object_cid(self):
        """Test CID generation for binary data using VBaseBytesObject
        and compare it with VBaseStringObject.
        """
        # Original string
        text = "This is some binary content from a string."

        # Convert to bytes
        data_bytes = text.encode("utf-8")

        # Generate CID from bytes
        vbo_bytes = VBaseBytesObject(init_data=data_bytes)
        cid_bytes = vbo_bytes.get_cid()

        # Generate CID from string
        vbo_str = VBaseStringObject(init_data=text)
        cid_str = vbo_str.get_cid()

        # Validate formats
        assert isinstance(cid_bytes, str)
        assert cid_bytes.startswith("0x")
        assert len(cid_bytes) == 66

        assert isinstance(cid_str, str)
        assert cid_str.startswith("0x")
        assert len(cid_str) == 66

        print(f"CID (bytes):  {cid_bytes}")
        print(f"CID (string): {cid_str}")

        # The CIDs should be equals
        assert cid_bytes is not None
        assert cid_str is not None
        assert cid_bytes == cid_str

    def test_image_cid_consistency(self):
        """Verify CID for image file is deterministic and matches manual SHA3-256."""
        image_bytes = create_test_image()
        expected_cid = sha3_256_hash_bytes(image_bytes)
        vbo = VBaseBytesObject(init_data=image_bytes)
        cid = vbo.get_cid()
        assert cid == expected_cid, f"Expected {expected_cid}, got {cid}"


if __name__ == "__main__":
    unittest.main()
