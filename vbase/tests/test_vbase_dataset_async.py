"""
Tests of the vbase_dataset_async module
"""

import asyncio
import logging
import time
import unittest

import pandas as pd

from vbase.core.vbase_client import VBaseClient
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.vbase_dataset_async import VBaseDatasetAsync
from vbase.core.vbase_object import VBaseIntObject
from vbase.tests.utils import dataset_add_record_checks, dataset_from_json_checks
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


# A short time period for a non-blocking operation.
# This is somewhat arbitrary, but should reliably
# filter fast non-blocking operations from
# slow blocking operations that take more than the interval.
_NON_BLOCKING_INTERVAL = 0.01


class TestVBaseDatasetAsync(unittest.TestCase):
    """
    Test base vBase dataset async functionality.
    """

    def setUp(self):
        """
        Set up the tests.
        """
        self.vbc = VBaseClientTest.create_instance_from_env()

    async def _add_record_worker_async(
        self,
        vbc: VBaseClient,
        dsw: VBaseDatasetAsync,
        record_data: any,
        t_prev: pd.Timestamp,
    ):
        # Test the async record creation.
        start_time = time.time()
        task = asyncio.create_task(dsw.add_record_async(record_data))
        elapsed_time = time.time() - start_time
        _LOG.info(
            "dsw.add_record_async(record_data) create_task took %s seconds.",
            elapsed_time,
        )
        self.assertTrue(elapsed_time < _NON_BLOCKING_INTERVAL)

        # Test the await for record creation.
        start_time = time.time()
        cl = await task
        elapsed_time = time.time() - start_time
        _LOG.info(
            "dsw.add_record_async(record_data) await took %s seconds.", elapsed_time
        )
        self.assertTrue(elapsed_time > _NON_BLOCKING_INTERVAL)

        dataset_add_record_checks(vbc, dsw, cl, t_prev)
        return cl, cl["timestamp"]

    async def dataset_creation_async(self):
        """
        Test simple dataset creation async worker
        """
        dataset_name = "TestDataset"
        self.vbc.clear_sets()
        self.vbc.clear_named_set_objects(dataset_name)

        # Test the async dataset creation.
        # We need to call the async VBaseDatasetAsync.create() factory method.
        start_time = time.time()
        task = asyncio.create_task(
            VBaseDatasetAsync.create(
                self.vbc, name=dataset_name, record_type=VBaseIntObject
            )
        )
        elapsed_time = time.time() - start_time
        _LOG.info(
            "VBaseDatasetAsync.create() create_task took %s seconds.", elapsed_time
        )
        self.assertTrue(elapsed_time < _NON_BLOCKING_INTERVAL)

        # Test the await for dataset creation.
        start_time = time.time()
        dsw = await task
        elapsed_time = time.time() - start_time
        _LOG.info("VBaseDatasetAsync.create() await took %s seconds.", elapsed_time)
        self.assertTrue(elapsed_time > _NON_BLOCKING_INTERVAL)
        self.assertTrue(self.vbc.verify_user_sets(dsw.owner, dsw.cid))
        return dsw

    def test_dataset_creation_async(self):
        """
        Test simple dataset creation
        """
        # In the unittest framework, test methods are expected to be synchronous functions.
        # unittest doesn't natively support async test methods.
        # Define a synchronous test method and call asyncio.run to run the asynchronous code.
        asyncio.run(self.dataset_creation_async())

    async def dataset_1d_int_ts_wr_async(self):
        """
        Test a simple int 1-dimension timeseries write and read async worker
        """
        dsw = await self.dataset_creation_async()
        t_prev = 0
        for i in range(1, 5):
            # Commit an integer.
            cl, t_prev = await self._add_record_worker_async(self.vbc, dsw, i, t_prev)
            assert cl is not None
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

    def test_dataset_1d_int_ts_wr_async(self):
        """
        Test a simple int 1-dimension timeseries write and read
        """
        asyncio.run(self.dataset_1d_int_ts_wr_async())


if __name__ == "__main__":
    unittest.main()
