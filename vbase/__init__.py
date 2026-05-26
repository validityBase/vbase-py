"""vbase

A Python library for interacting with the validityBase (vBase) platform
"""

from vbase._version import __version__
from vbase.core.aggregate_indexing_service import AggregateIndexingService
from vbase.core.failover_indexing_service import FailoverIndexingService
from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.forwarder_commitment_service_test import ForwarderCommitmentServiceTest
from vbase.core.indexing_service import IndexingService, Web3HTTPIndexingService
from vbase.core.sql_indexing_service import SQLIndexingService
from vbase.core.vbase_client import VBaseClient
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.vbase_dataset import (
    VBaseDataset,
)
from vbase.core.vbase_dataset_async import (
    VBaseDatasetAsync,
)
from vbase.core.set_matching import (
    ChainSetMatchingService,
    BaseSetMatchingService,
    FuzzySetMatchingService,
    HeadBasedSetMatchingService,
    SetMatch,
    SetMatchingCriteria,
    TimestampedCid,
)
from vbase.core.vbase_object import (
    VBaseBytesObject,
    VBaseFloatObject,
    VBaseIntObject,
    VBaseJsonObject,
    VBaseObject,
    VBasePortfolioObject,
    VBasePrivateFloatObject,
    VBasePrivateIntObject,
    VBaseStringObject,
)
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.core.web3_http_commitment_service_test import Web3HTTPCommitmentServiceTest
from vbase.utils.log import get_default_logger

__all__ = [
    "__version__",
    "VBaseClient",
    "VBaseClientTest",
    "Web3HTTPCommitmentService",
    "Web3HTTPCommitmentServiceTest",
    "ForwarderCommitmentService",
    "ForwarderCommitmentServiceTest",
    "IndexingService",
    "Web3HTTPIndexingService",
    "SQLIndexingService",
    "ChainSetMatchingService",
    "HeadBasedSetMatchingService",
    "FuzzySetMatchingService",
    "BaseSetMatchingService",
    "SetMatch",
    "SetMatchingCriteria",
    "TimestampedCid",
    "FailoverIndexingService",
    "AggregateIndexingService",
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
