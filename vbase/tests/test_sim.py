"""
Test Point-in-time (PIT) simulations
Such simulations allow calculations to run in a simulated PIT environment
accessing committed data precisely as it existed historically.
"""

import logging

import time
import unittest
import numpy as np
import pandas as pd

import vbase.core.vbase_client
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.vbase_object import VBaseIntObject
from vbase.utils.log import get_default_logger
from vbase.tests.utils import (
    create_dataset_worker,
    dataset_from_json_checks,
)


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


# Force verbose vbase logging output for this test.
# noinspection PyProtectedMember
vbase.core.vbase_client.LOG.setLevel(logging.DEBUG)


class TestSim(unittest.TestCase):
    """
    Test vBase simulator APIs.
    """

    def setUp(self):
        """
        Set up the tests.
        """
        self.vbc = VBaseClientTest.create_instance_from_env()

    def test_5t_sim(self):
        """
        Test a simple simulation over 5 timestamps.
        """

        # Record 5 int commitments.
        # This creates the following test dataset:
        #        t    data
        # 2023/1/1       1
        #        2       4
        #        3       9
        #        4      16
        #        5      25
        dsw = create_dataset_worker(self.vbc, VBaseIntObject)
        cl = None
        ts = []
        for x in range(1, 6):
            t = pd.Timestamp(f"2023/01/{x}", tz="UTC")
            ts.append(t)
            cl = dsw.add_record_with_timestamp(x, t)
            assert self.vbc.verify_user_object(dsw.owner, cl["objectCid"], t)
            assert self.vbc.verify_user_set_objects(
                dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
            )
            assert cl["timestamp"] == self.vbc.normalize_pd_timestamp(t)
            time.sleep(1)
        assert self.vbc.verify_user_set_objects(
            dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum))
        )
        dataset_from_json_checks(self.vbc, dsw)

        # Run a simulation invoking a PIT calculation on the latest record for each sim t.
        def callback() -> int:
            """
            Sim callback
            """
            record_data = dsw.get_last_record_data()
            _LOG.info("callback(): record_data = %s", record_data)
            assert isinstance(record_data, int)
            ret = record_data**2
            _LOG.info("callback(): ret = %s", ret)
            return ret

        s_sim = self.vbc.run_pit_sim(ts=pd.DatetimeIndex(ts), callback=callback)
        _LOG.info("self.vbase.run_pit_sim(): ret =\n%s", s_sim)
        assert s_sim.equals(pd.Series(np.array(range(1, 6)) ** 2, index=ts))


if __name__ == "__main__":
    unittest.main()
