"""vBase Test utils"""

import json
import logging
import os
import pprint
import time
from typing import Optional, Type

from vbase.core.commitment_service import CommitmentService
from vbase.core.forwarder_commitment_service import ForwarderCommitmentService
from vbase.core.indexing_service import Web3HTTPIndexingService
from vbase.core.vbase_client import VBaseClient
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.vbase_dataset import VBaseDataset
from vbase.core.vbase_object import VBaseObject
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


_LOCALHOST_RPC_ENDPOINT = "http://127.0.0.1:8545/"


def int_to_hash(n: int) -> str:
    """Convert an integer to a hash string.

    :param n: The integer.
    :return: The resulting hash string.
    """
    return "0x" + f"{n:X}".rjust(64, "0")


# Hash constants used in various tests.
TEST_HASH1 = int_to_hash(1)
TEST_HASH2 = int_to_hash(100)


def create_test_indexing_service(
    commitment_service: CommitmentService,
) -> Web3HTTPIndexingService:
    """Build a test indexer using an explicit RPC endpoint for forwarders.

    The endpoint must serve the forwarder's chain and support the indexing
    methods of the package under test. See internal/specs/development.md.
    """
    node_rpc_url: Optional[str] = None
    if isinstance(commitment_service, ForwarderCommitmentService):
        # A secret saved with a trailing newline would otherwise reach requests.
        node_rpc_url = os.environ.get("VBASE_TEST_INDEXING_NODE_RPC_URL", "").strip()
        if not node_rpc_url:
            raise EnvironmentError(
                "VBASE_TEST_INDEXING_NODE_RPC_URL is required for forwarder "
                "indexing tests; supply an archival RPC endpoint on the "
                "forwarder's chain."
            )
    indexing_service = Web3HTTPIndexingService.create_instance_from_commitment_service(
        commitment_service, node_rpc_url=node_rpc_url
    )
    n_last_blocks = os.environ.get("VBASE_TEST_INDEXING_N_LAST_BLOCKS")
    if n_last_blocks:
        indexing_service.n_last_blocks = int(n_last_blocks)
    return indexing_service


def create_dataset_worker(
    vbc: VBaseClientTest,
    record_type: Type[VBaseObject],
    dataset_name: str = "TestDataset",
) -> VBaseDataset:
    """Common test code for test and dataset init.

    :param vbc: The vBase client object.
    :param record_type: The dataset record type.
    :param dataset_name: The dataset name.
    :returns dsw: The created dataset.
    """
    # Clear any stale dataset commitments from prior tests.
    vbc.clear_sets()
    # Clear any stale object commitments from prior tests.
    vbc.clear_named_set_objects(dataset_name)

    # Create the test dataset to write.
    dsw = VBaseDataset(vbc, name=dataset_name, record_type=record_type)
    assert vbc.verify_user_sets(dsw.owner, dsw.cid)

    return dsw


def dataset_add_record_checks(
    vbc: VBaseClient, dsw: VBaseDataset, cl: dict, t_prev: str
):
    """Common test code for dataset record checks.

    :param vbc: The vBase client object.
    :param dsw: The vBase dataset object.
    :param cl: The commitment log received from the commitment call.
    :param t_prev: Time of the previous operation.
    """
    _LOG.info("dataset_add_record_checks(): cl = %s", pprint.pformat(cl))
    assert vbc.verify_user_object(dsw.owner, cl["objectCid"], cl["timestamp"])
    assert vbc.verify_user_set_objects(dsw.owner, dsw.cid, str(hex(dsw.object_cid_sum)))
    # Use an internal function to quickly build timestamps.
    # noinspection PyUnresolvedReferences
    assert (
        vbc.commitment_service.convert_timestamp_str_to_chain(cl["timestamp"])
        - vbc.commitment_service.convert_timestamp_str_to_chain(t_prev)
    ) >= 1
    time.sleep(2)


def dataset_from_json_checks(vbc: VBaseClient, dsw: VBaseDataset, verbose: bool = True):
    """Common test code for dataset init from JSON.

    :param vbc: The vBase client object.
    :param dsw: The source dataset used to initialize the new dataset.
    :param verbose: True if checks should print output; False otherwise.
    """
    # Create the string descriptor of the written dataset.
    str_dataset_json = dsw.to_json()
    if verbose:
        _LOG.info(
            "str_dataset_json = %s",
            pprint.pformat(json.loads(str_dataset_json)).rstrip(),
        )
    # Create the new dataset from the JSON string to test the read, and verify it.
    dsr = VBaseDataset(vbc, init_json=str_dataset_json)
    assert dsr.verify_commitments()
    assert dsw.to_json() == dsr.to_json()
    if verbose:
        _LOG.info("dsr.get_pd_data_frame() =\n%s", dsr.get_pd_data_frame())
    else:
        _ = dsr.get_pd_data_frame()


def compare_dict_subset(superset_dict: dict, subset_dict: dict):
    """Compare a dictionary to a subset.

    :param superset_dict: The superset dictionary.
    :param subset_dict: The subset dictionary.
    """
    return {k: superset_dict[k] for k in subset_dict.keys()} == subset_dict
