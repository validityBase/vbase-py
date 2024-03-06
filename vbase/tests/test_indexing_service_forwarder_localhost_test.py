"""
Tests of the indexing service for the vbase package
Tests rely on MongoDB for name resolution and data availability.
"""

from vbase.tests.test_indexing_service import TestIndexingService
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.indexing_service import IndexingService


class TestIndexingServiceForwarderLocalhostTest(TestIndexingService):
    """
    Test base vBase indexing functionality using a local test node.
    """

    def setUp(self):
        dotenv_path = ".env.forwarder.localhost.test"
        self.vbc = VBaseClientTest.create_instance_from_env(dotenv_path)
        self.indexing_service = IndexingService.create_instance_from_commitment_service(
            self.vbc.commitment_service
        )
        super().setUp()
