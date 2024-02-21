"""
Tests of the vbase_dataset module using a local forwarder
Tests rely on MongoDB for name resolution and data availability.
"""

from vbase.core.vbase_client_test import VBaseClientTest
from vbase.tests.test_vbase_dataset import TestVBaseDataset


class TestVBaseDatasetForwarder(TestVBaseDataset):
    """
    Test basic vBase client functionality using a localhost forwarder.
    All test cases are inherited from TestVBaseClient.
    Tests rely on the .env defined in the proper test file.
    """

    def setUp(self):
        dotenv_path = ".env.forwarder.localhost.test"
        self.vbc = VBaseClientTest.create_instance_from_env(dotenv_path)
        super().setUp()
