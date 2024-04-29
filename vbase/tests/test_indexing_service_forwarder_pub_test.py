"""
Tests of the indexing service for the vbase package using a public test forwarder
Tests rely on MongoDB for name resolution and data availability.
"""

from vbase.tests.test_indexing_service import TestIndexingService
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.indexing_service import IndexingService


class TestIndexingServiceForwarderLocalhostTest(TestIndexingService):
    """
    All test cases are inherited from TestIndexingService.
    Tests rely on the .env defined in the proper test file.
    We could pass these argument as command line or set them as environment variables,
    but wrapping the whole thing in a .py file is most convenient for command line
    and Pycharm runs.
    """

    def setUp(self):
        dotenv_path = ".env.forwarder.pub.test"
        self.vbc = VBaseClientTest.create_instance_from_env(dotenv_path)
        self.indexing_service = IndexingService.create_instance_from_commitment_service(
            self.vbc.commitment_service
        )
        super().setUp()
