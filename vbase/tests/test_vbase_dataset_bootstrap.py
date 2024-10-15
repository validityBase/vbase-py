"""
Tests of the large dataset bootstrap for the vbase module
"""

import time
import unittest

from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.vbase_object import VBasePortfolioObject
from vbase.tests.utils import (
    create_dataset_worker,
    dataset_from_json_checks,
)


class TestVBaseDatasetBootstrap(unittest.TestCase):
    """
    Test base vBase dataset bootstrap functionality.
    """

    def setUp(self):
        """
        Set up the tests.
        """
        self.vbc = VBaseClientTest.create_instance_from_env()

    def test_port_wr(self):
        """
        Test timeseries of portfolio records write and read.
        """
        dsw = create_dataset_worker(self.vbc, VBasePortfolioObject)

        # Record 5 commitments over 5 blocks.
        # We should get a new block with each commitment due to automine.
        # Sleep briefly to make sure timestamps don't collide.
        cl = None
        for i in range(1, 6):
            # Commit a JSON object.
            data = dict(zip(["AAA", "BBB", "CCC"], [0.2, 0.3 + i / 100, 0.5 - i / 100]))
            # Use an internal function to quickly build timestamps.
            # noinspection PyUnresolvedReferences
            ts = self.vbc.commitment_service.convert_timestamp_chain_to_str(i)
            cl = dsw.add_record_with_timestamp(data, ts)
            assert self.vbc.verify_user_object(dsw.owner, cl["objectCid"], ts)
            assert self.vbc.verify_user_set_objects(
                dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
            )
            assert cl["timestamp"] == ts
            time.sleep(1)
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )

        dataset_from_json_checks(self.vbc, dsw)

    def test_port_wr_batch(self):
        """
        Test timeseries of portfolio records write and read using a batch commitment.
        """
        dsw = create_dataset_worker(self.vbc, VBasePortfolioObject)

        # Record 5 commitments over 5 blocks.
        # We should get a new block with each commitment due to automine.
        # Sleep briefly to make sure timestamps don't collide.
        records = []
        timestamps = []
        for i in range(1, 6):
            # Commit a JSON object.
            records.append(
                dict(zip(["AAA", "BBB", "CCC"], [0.2, 0.3 + i / 100, 0.5 - i / 100]))
            )
            # Use an internal function to quickly build timestamps.
            # noinspection PyUnresolvedReferences
            timestamps.append(
                self.vbc.commitment_service.convert_timestamp_chain_to_str(i)
            )
        cl = dsw.add_records_with_timestamps_batch(records, timestamps)
        assert cl is not None
        for i in range(0, 5):
            assert self.vbc.verify_user_object(
                dsw.owner, cl[i]["objectCid"], timestamps[i]
            )
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )

        dataset_from_json_checks(self.vbc, dsw)


if __name__ == "__main__":
    unittest.main()
