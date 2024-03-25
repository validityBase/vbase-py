"""
Tests of the vbase_dataset module using a local forwarder
Tests rely on MongoDB for name resolution and data availability.
"""

from vbase.core.vbase_client import VBaseClient
from vbase.tests.test_vbase_dataset import TestVBaseDataset


class TestVBaseDatasetForwarder(TestVBaseDataset):
    """
    Test base vBase dataset functionality using a forwarder and a private key.
    All test cases are inherited from TestVBaseDataset.
    Tests require .env to define the environment variables for connecting to the forwarder:
        VBASE_COMMITMENT_SERVICE_CLASS
        VBASE_FORWARDER_URL
        VBASE_API_KEY
        VBASE_COMMITMENT_SERVICE_PRIVATE_KEY
    """

    def setUp(self):
        """
        Set up the tests.
        Uses test configuration from .env.
        """
        dotenv_path = ".env"
        self.vbc = VBaseClient.create_instance_from_env(dotenv_path)
        super().setUp()
