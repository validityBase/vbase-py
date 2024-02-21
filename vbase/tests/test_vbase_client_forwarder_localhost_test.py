"""
Tests of the vbase module using a local forwarder
"""

from vbase.core.vbase_client_test import VBaseClientTest
from vbase.tests.test_vbase_client import TestVBaseClient


class TestVBaseClientForwarderLocalhostTest(TestVBaseClient):
    """
    All test cases are inherited from TestVBaseClient.
    Tests rely on the .env defined in the proper test file.
    We could pass these argument as command line or set them as environment variables,
    but wrapping the whole thing in a .py file is most convenient for command line
    and Pycharm runs.
    """

    def setUp(self):
        dotenv_path = ".env.forwarder.localhost.test"
        self.vbc = VBaseClientTest.create_instance_from_env(dotenv_path)
        super().setUp()
