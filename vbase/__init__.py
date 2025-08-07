"""
vbase

A Python library for interacting with the validityBase (vBase) platform
"""

from vbase.core.vbase_client import VBaseClient
from vbase.core.vbase_client_test import VBaseClientTest

from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.core.web3_http_commitment_service_test import Web3HTTPCommitmentServiceTest
from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.forwarder_commitment_service_test import ForwarderCommitmentServiceTest

from vbase.core.indexing_service import (
    IndexingService,
    Web3HTTPIndexingService
)

from vbase.core.sql_indexing_service import (
    SQLIndexingService
)

from vbase.core.failover_indexing_service import (
    FailoverIndexingService
)

from vbase.core.vbase_dataset import (
    VBaseDataset,
)

from vbase.core.vbase_dataset_async import (
    VBaseDatasetAsync,
)

from vbase.core.vbase_object import (
    VBaseObject,
    VBaseIntObject,
    VBasePrivateIntObject,
    VBaseFloatObject,
    VBasePrivateFloatObject,
    VBaseStringObject,
    VBaseJsonObject,
    VBasePortfolioObject,
    VBaseBytesObject,
)

from vbase.utils.log import get_default_logger

__all__ = [
    "VBaseClient",
    "VBaseClientTest",
    "Web3HTTPCommitmentService",
    "Web3HTTPCommitmentServiceTest",
    "ForwarderCommitmentService",
    "ForwarderCommitmentServiceTest",
    "IndexingService",
    "Web3HTTPIndexingService",
    "SQLIndexingService",
    "FailoverIndexingService",
    "VBaseObject",
    "VBaseIntObject",
    "VBasePrivateIntObject",
    "VBaseFloatObject",
    "VBasePrivateFloatObject",
    "VBaseStringObject",
    "VBaseJsonObject",
    "VBasePortfolioObject",
    "VBaseBytesObject",
    "VBaseDataset",
    "VBaseDatasetAsync",
    "get_default_logger",
]
