"""Test Point-in-time (PIT) simulations
Such simulations allow calculations to run in a simulated PIT environment
accessing committed data precisely as it existed historically.
"""

import logging
import threading
import time
import unittest
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

import vbase.core.vbase_client
from vbase.core.commitment_service import CommitmentService
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.vbase_object import VBaseIntObject
from vbase.tests.utils import create_dataset_worker, dataset_from_json_checks
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


# Force verbose vbase logging output for this test.
# noinspection PyProtectedMember
vbase.core.vbase_client.LOG.setLevel(logging.DEBUG)


class TestSim(unittest.TestCase):
    """Test vBase simulator APIs."""

    def setUp(self):
        """Set up the tests."""
        self.vbc = VBaseClientTest.create_instance_from_env()

    def test_5t_sim(self):
        """Test a simple simulation over 5 timestamps."""
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
            """Sim callback"""
            record_data = dsw.get_last_record_data()
            _LOG.info("callback(): record_data = %s", record_data)
            assert isinstance(record_data, int)
            ret = record_data**2
            _LOG.info("callback(): ret = %s", ret)
            return ret

        s_sim = self.vbc.run_pit_sim(ts=pd.DatetimeIndex(ts), callback=callback)
        _LOG.info("self.vbase.run_pit_sim(): ret =\n%s", s_sim)
        assert s_sim.equals(pd.Series(np.array(range(1, 6)) ** 2, index=ts))


class TestSimState(unittest.TestCase):
    """Test the simulation state of VBaseClient without a commitment service."""

    def test_callback_error_leaves_simulation_mode(self):
        """A callback error propagates, and the client leaves simulation mode."""
        vbc = vbase.core.vbase_client.VBaseClient(MagicMock(spec=CommitmentService))
        ts = pd.date_range("2023-01-01", periods=3, freq="D", tz="UTC")

        def callback():
            if vbc.get_sim_t() == ts[1]:
                raise ValueError("callback failed")
            return 1

        with self.assertRaisesRegex(ValueError, "callback failed"):
            vbc.run_pit_sim(ts=ts, callback=callback)

        self.assertFalse(vbc.in_sim())
        self.assertIsNone(vbc.get_sim_t())

    def test_nested_simulation_restores_outer_state(self):
        """A simulation run from a callback restores the enclosing simulation."""

        def succeeding_inner():
            return 1

        def failing_inner():
            raise ValueError("inner callback failed")

        for inner in (succeeding_inner, failing_inner):
            with self.subTest(inner=inner.__name__):
                self._check_nested_simulation(inner)

    def _check_nested_simulation(self, inner):
        """Run a simulation whose callback runs another one with ``inner``."""
        vbc = vbase.core.vbase_client.VBaseClient(MagicMock(spec=CommitmentService))
        outer_ts = pd.date_range("2023-01-01", periods=2, freq="D", tz="UTC")
        inner_ts = pd.date_range("2022-01-01", periods=2, freq="D", tz="UTC")
        states = []

        def outer():
            try:
                vbc.run_pit_sim(ts=inner_ts, callback=inner)
            except ValueError:
                pass
            states.append((vbc.in_sim(), vbc.get_sim_t()))
            return 1

        vbc.run_pit_sim(ts=outer_ts, callback=outer)

        self.assertEqual(states, [(True, outer_ts[0]), (True, outer_ts[1])])
        self.assertFalse(vbc.in_sim())
        self.assertIsNone(vbc.get_sim_t())

    def test_overlapping_simulations_end_out_of_simulation(self):
        """Simulations that overlap on two threads end out of simulation mode."""
        vbc = vbase.core.vbase_client.VBaseClient(MagicMock(spec=CommitmentService))
        a_in_callback = threading.Event()
        b_in_callback = threading.Event()
        a_returned = threading.Event()

        def callback_a():
            a_in_callback.set()
            b_in_callback.wait(timeout=5)
            return 1

        def callback_b():
            b_in_callback.set()
            a_returned.wait(timeout=5)
            return 2

        def run_a():
            vbc.run_pit_sim(
                ts=pd.DatetimeIndex([pd.Timestamp("2023-01-01", tz="UTC")]),
                callback=callback_a,
            )
            a_returned.set()

        # A starts first and returns while B is still in its callback.
        thread_a = threading.Thread(target=run_a)
        thread_b = threading.Thread(
            target=vbc.run_pit_sim,
            kwargs={
                "ts": pd.DatetimeIndex([pd.Timestamp("2024-01-01", tz="UTC")]),
                "callback": callback_b,
            },
        )
        thread_a.start()
        self.assertTrue(a_in_callback.wait(timeout=5))
        thread_b.start()
        thread_a.join(timeout=10)
        thread_b.join(timeout=10)

        self.assertTrue(a_returned.is_set())
        self.assertFalse(vbc.in_sim())
        self.assertIsNone(vbc.get_sim_t())


if __name__ == "__main__":
    unittest.main()
