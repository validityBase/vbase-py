"""
Dataset support for the validityBase (vBase) platform.
A vBase dataset comprises one or more records (objects) belonging to a set.
"""

from abc import ABC
from enum import Enum
import json
import logging
from typing import Any, List, Type, Union
import numpy as np
import pandas as pd

from vbase.core.vbase_client import VBaseClient
from vbase.core.vbase_client_test import VBaseClientTest
from vbase.core.vbase_object import VBaseObject, VBASE_OBJECT_TYPES
from vbase.core.indexing_service import IndexingService
from vbase.utils.crypto_utils import (
    add_int_uint256,
    solidity_hash,
)
from vbase.utils.log import get_default_logger


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class _Access(Enum):
    """
    Encodes the type of dataset access.
    Dataset access has similar categories as FS file access.
    New datasets are created and opened with Write access and can be modified.
    Existing datasets that a user owns can also be opened with Write access.
    External datasets that a user does not own or does not wish to modify
    are opened with read access.
    TODO: Consider adding additional access abstractions to cover the following scenarios:
    - Dataset creator can append to it
    - Dataset creator and reader can read and verify it
    Simple read/write access may not map well to the above.
    """

    WRITE = "write"
    READ = "read"


class VBaseDataset(ABC):
    """
    Provides Python vBase dataset access.
    Implements base functionality shared across datasets regardless of record type.
    Record-specific logic is implemented in the record class.
    """

    # The class is the entry point to dataset operations.
    # It has lots of instance attributes by design.
    # Disable R0902: Too many instance attributes (8/7) (too-many-instance-attributes)
    # pylint: disable=R0902

    def _create_new_if_necessary(
        self,
        name: str,
        record_type: Type[VBaseObject],
    ):
        """
        Create a new VBaseDataset object with a given name, if necessary.
        Most vBase workflows append records to a dataset with a given name.
        They need a dataset created if one does not exist yet.

        :param name: Name of the dataset to create.
        """
        self.access = _Access.WRITE
        self.name = name
        self.record_type = record_type
        self.record_type_name = self.record_type.__name__
        # Verify that the name is for a known record_type.
        assert self.record_type_name in VBASE_OBJECT_TYPES
        self.owner = self.vbc.get_default_user()
        self.cid = self.get_set_cid_for_dataset(name)
        # If the dataset with the given name does not yet exist for the user, create it.
        if not self.vbc.user_named_set_exists(self.vbc.get_default_user(), name):
            cl = self.vbc.add_named_set(name)
            assert cl["user"] == self.owner and cl["setCid"] == self.cid

    def _init_from_dict(self, init_dict: dict):
        """
        Initialize a new VBaseDataset object using a dict.

        :param init_dict: VBaseDataset dictionary representation.
        """
        self.access = _Access.READ
        self.name = init_dict["name"]
        self.record_type_name = init_dict["record_type_name"]
        assert self.record_type_name in VBASE_OBJECT_TYPES
        self.record_type: Type[VBaseObject] = VBASE_OBJECT_TYPES.get(
            self.record_type_name
        )
        self.owner = init_dict["owner"]
        self.cid = self.get_set_cid_for_dataset(self.name)
        # Initialize records using init_dicts since we saved them as dicts.
        self.records = [
            self.record_type(init_dict=record_init_dict)
            for record_init_dict in init_dict["records"]
        ]
        # Timestamps are optional.
        # These can be retrieved from commitment receipts and rebuilt
        # if needed.
        if "timestamps" in init_dict:
            self.timestamps = init_dict["timestamps"]

    def _init_from_json(self, init_json: str):
        """
        Initialize a new VBaseDataset object using JSON.

        :param init_json: VBaseDataset JSON representation.
        """
        init_dict = json.loads(init_json)
        self._init_from_dict(init_dict)

    # The dataset constructor has to have many arguments
    # to support multiple initialization modes and overloading.
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        vbc: Union[VBaseClient, VBaseClientTest],
        name: Union[str, None] = None,
        record_type: Union[Type[VBaseObject], None] = None,
        init_dict: Union[dict, None] = None,
        init_json: Union[str, None] = None,
    ):
        """
        Create a VBaseDataset handle.
        The handle references a vBase dataset.
        The handle creates and opens a new dataset or
        accesses an existing dataset.
        TODO: Finalize the modes of dataset creation, append, and verification.
        The current implementation is stable for dataset creation.

        :param vbc: The vBase object used to access commitments.
        :param name: The name of the new dataset.
        :param record_type: The type of dataset records.
            To create a new dataset in Write mode, name and record_type must be specified.
        :param init_dict: Dictionary dataset representation.
            If specified, an existing dataset is opened with Read access.
        :param init_json: VBaseDataset JSON representation
            If specified, an existing dataset is opened with Read access.
        """
        self.vbc = vbc

        # Fields describing the dataset.
        self.name: Union[str, None] = None
        self.record_type: Union[Type[VBaseObject], None] = None
        self.record_type_name: Union[str, None] = None
        self.owner: Union[str, None] = None
        self.cid: Union[str, None] = None
        self.indexing_service: Union[IndexingService, None] = None

        # Fields describing dataset (set) records (objects).
        self.records: List[any] = []
        # Timestamps need to be serializable.
        # pd.Timestamp is not serializable, so we save string representations of timestamps.
        self.timestamps: List[str] = []
        self.object_cid_sum: int = 0

        # Process dataset access type.
        self.access: Union[_Access, None] = None

        if name is not None:
            # Create a new writable dataset.
            assert record_type is not None
            assert init_dict is None
            assert init_json is None
            # This can be a new dataset or an existing one
            # that will be extended with new records.
            # Note that when appending to an existing dataset,
            # older records will not be pre-populated.
            # If we run into scenarios where this is a problem,
            # we can provide richer append functionality.
            self._create_new_if_necessary(name, record_type)
        elif init_dict is not None:
            # Open an existing dataset using a dictionary representation.
            assert init_json is None
            self._init_from_dict(init_dict)
        else:
            # Open an existing dataset using a JSON representation.
            assert init_json is not None
            self._init_from_json(init_json)

    def get_timestamps(self) -> pd.DatetimeIndex:
        """
        Get all record timestamps.

        :return: The timestamps for all dataset records.
        """
        return pd.DatetimeIndex([pd.Timestamp(t) for t in self.timestamps])

    def to_dict(self) -> dict:
        """
        Return dictionary representation of the dataset.

        :return: The dictionary representation of the dataset.
        """
        # Verify that the record_type we are saving can be mapped to a record class.
        assert self.record_type_name in VBASE_OBJECT_TYPES
        return {
            "name": self.name,
            "owner": self.owner,
            "cid": self.cid,
            "record_type_name": self.record_type_name,
            # Records are converted to dicts and must be loaded as such
            # from the dict representation.
            "records": [record.__dict__ for record in self.records],
            "timestamps": self.timestamps,
        }

    def to_json(self) -> str:
        """
        Return JSON representation of the dataset.

        :return: The JSON representation of the dataset.
        """
        return json.dumps(self.to_dict())

    @staticmethod
    def get_set_cid_for_dataset(dataset_name: str) -> str:
        """
        Generate set CID for a named dataset.
        May be called to post commitments without instantiating a dataset object.

        :param dataset_name: The dataset name.
        :return: The CID for the dataset.
        """
        return solidity_hash(["string"], [dataset_name]).hex()

    def _add_record_worker(
        self, record: VBaseObject, object_cid: str, timestamp: pd.Timestamp
    ):
        """
        Common code to add record to a VBaseDataset object.

        :param record: The dataset record.
        :param object_cid: The CID of the record.
        :param timestamp: The record timestamp.
        """
        assert self.access == _Access.WRITE
        # Add uint256s with overflow and wrap-around.
        # This replicates the following sol code:
        # unchecked {
        #    userSetObjectCidSums[userSetCid] += uint256(objectCid);
        # }
        self.object_cid_sum = add_int_uint256(self.object_cid_sum, object_cid)
        self.records.append(record)
        self.timestamps.append(str(timestamp))

    def add_record(self, record_data: any) -> dict:
        """
        Add a record to a VBaseDataset object.

        :param record_data: The record datum.
        :return: The commitment log containing commitment receipt info.
        """
        assert self.access == _Access.WRITE
        record = self.record_type(record_data)
        object_cid = record.get_cid()
        cl = self.vbc.add_set_object(self.cid, object_cid)
        self._add_record_worker(record, object_cid, cl["timestamp"])
        return cl

    def add_records_batch(self, record_data_list: List[any]) -> List[dict]:
        """
        Add a list of records to a VBaseDataset object.
        This function will typically be called to backfill a dataset history:
        - Producer creates records for 1/1, 1/2, 1/3.
        - On 1/4 methodology changes.
        - Producer back-fills history for 1/1, 1/2, 1/3 and adds their commitments.

        :param record_data_list: The list of records' data.
        :return: The commitment log list containing commitment receipts.
        """
        assert self.access == _Access.WRITE
        # Build and submit commitment to get the timestamps.
        records = [self.record_type(record_data) for record_data in record_data_list]
        object_cids = [record.get_cid() for record in records]
        cls = self.vbc.add_set_objects_batch(self.cid, object_cids)
        # Record the timestamps received from the commitment receipt.
        for i, record in enumerate(records):
            self._add_record_worker(record, object_cids[i], cls[i]["timestamp"])
        return cls

    def add_record_with_timestamp(
        self, record_data: any, timestamp: Union[pd.Timestamp, str]
    ) -> dict:
        """
        Test shim to add a record to a VBaseDataset object with a given timestamp.
        Only supported by test contracts.

        :param record_data: The record datum.
        :param timestamp: Timestamp to force for the record.
        :return: The commitment log containing commitment receipt info.
        """
        assert self.access == _Access.WRITE
        record = self.record_type(record_data)
        timestamp = self.vbc.normalize_pd_timestamp(timestamp)
        object_cid = record.get_cid()
        cl = self.vbc.add_set_object_with_timestamp(self.cid, object_cid, timestamp)
        assert cl["timestamp"] == timestamp
        self._add_record_worker(record, object_cid, timestamp)
        return cl

    def add_records_with_timestamps_batch(
        self,
        record_data_list: List[any],
        timestamps: List[Union[pd.Timestamp, str]],
    ) -> List[dict]:
        """
        Test shim to add a batch of records with timestamps to a VBaseDataset object.
        Only supported by test contracts.

        :param record_data_list: The list of records' data.
        :param timestamps: The list of timestamps to force for the records.
        :return: The commitment log list containing commitment receipts.
        """
        assert self.access == _Access.WRITE
        # Build and submit commitment to get the timestamps.
        records = [self.record_type(record_data) for record_data in record_data_list]
        object_cids = [record.get_cid() for record in records]
        timestamps = [self.vbc.normalize_pd_timestamp(t) for t in timestamps]
        cls = self.vbc.add_sets_objects_with_timestamps_batch(
            [self.cid] * len(records), object_cids, timestamps
        )
        # Record the timestamps received from the commitment receipt.
        for i, record in enumerate(records):
            self._add_record_worker(record, object_cids[i], cls[i]["timestamp"])
        for i, record in enumerate(records):
            assert cls[i]["timestamp"] == timestamps[i]
        return cls

    def get_records(self) -> Union[List[any], None]:
        """
        Get all records for the dataset.

        :return: All record up to the current time:
            - If not in a simulation, returns all records.
            - If within a simulation, returns records with a timestamp less
            than or equal to the sim t.
            - If all records are after the current time, returns None.
        """
        if self.vbc.in_sim():
            t = self.vbc.get_sim_t()
            assert t is not None
            inds_match = np.where(np.array(self.timestamps) <= t.timestamp())[0]
            if len(inds_match) == 0:
                return None
        else:
            inds_match = np.arange(len(self.records))
        return [self.records[i] for i in inds_match]

    def get_last_record(self) -> Union[Any, None]:
        """
        Get the last/latest record for the dataset.

        :return: The last/latest record prior to the current time:
            - If not in a simulation, this is the last known record.
            - If within a simulation, this is the last record with a timestamp
            less than or equal to the sim t.
            - If all records are after the current time, returns None.
        """
        if self.vbc.in_sim():
            t = self.vbc.get_sim_t()
            assert t is not None
            inds_match = np.where(
                np.array(self.timestamps) <= self.vbc.normalize_pd_timestamp(t)
            )[0]
            if len(inds_match) == 0:
                return None
            return self.records[max(inds_match)]
        return self.records[-1]

    def get_last_record_data(self) -> Union[Any, None]:
        """
        Get the last/latest record's data for the dataset.

        :return: The last/latest record data prior to the current time
            using get_last_record() semantics.
        """
        record = self.get_last_record()
        return record.data

    def verify_commitments(self) -> (bool, List[str]):
        """
        Verify commitments for all dataset records.

        :return: A tuple containing success and log:
            - success: True if all record commitments have been verified; False otherwise.
            - l_log: A list log of verification explaining any failures.
        """
        success = True
        l_log = []

        self.cid = self.get_set_cid_for_dataset(self.name)
        assert len(self.records) == len(self.timestamps)

        self.object_cid_sum = 0
        for i, record in enumerate(self.records):
            timestamp = self.timestamps[i]
            object_cid = record.get_cid()
            self.object_cid_sum = add_int_uint256(self.object_cid_sum, object_cid)
            if not self.vbc.verify_user_object(self.owner, object_cid, timestamp):
                l_log.append(
                    "Invalid record: "
                    f"Failed object verification: "
                    f"owner = {self.owner}, "
                    f"timestamp = {timestamp}, "
                    f"object_cid = {object_cid}"
                )
                success = False

        str_object_cid_sum = str(hex(self.object_cid_sum))
        if not self.vbc.verify_user_set_objects(
            self.owner, self.cid, str_object_cid_sum
        ):
            l_log.append(
                "Invalid records: "
                "Failed object set verification: "
                f"owner = {self.owner}, "
                f"set_cid = {self.cid}, "
                f"str_object_cid_sum = {str_object_cid_sum}"
            )
            success = False

        return success, l_log

    def get_commitment_receipts(self) -> List[dict]:
        """
        Get commitment receipts for dataset records.

        :return: Commitment receipts for dataset records.
        """
        self.cid = self.get_set_cid_for_dataset(self.name)

        # Create the indexing service object using the commitment service.
        if self.indexing_service is None:
            self.indexing_service = (
                IndexingService.create_instance_from_commitment_service(
                    self.vbc.commitment_service
                )
            )
        # Find the commitment receipts for the set.
        commitment_receipts = self.indexing_service.find_user_set_objects(
            user=self.owner, set_cid=self.cid
        )

        return commitment_receipts

    def try_restore_timestamps_from_index(self) -> (bool, List[str]):
        """
        Try to restore timestamps for dataset records using the index service.

        The function should always attempt to do the right thing by default,
        but long-term options can get complex. The following work remains:
        - How should records with identical CIDs be treated?
        - When multiple commitments exist for a given CID, what is the order of pairing?

        :return: A tuple containing success and log:
            - success: True if all record have been found in the index
                and timestamps restored; False otherwise.
            - l_log: A list log of verification explaining any failures.
        """
        success = True
        l_log = []

        commitment_receipts = self.get_commitment_receipts()

        # We may be fixing existing timestamps or adding new ones.
        assert len(self.timestamps) == len(self.records) or len(self.timestamps) == 0
        if len(self.timestamps) == 0:
            self.timestamps = [None] * len(self.records)

        # Fix the timestamps using the commitment receipts.
        # Traverse all the records.
        # The following algorithm is simplistic O(n^2).
        # We can make this something more complex and performant it this becomes an issue.
        for i, ds_record in enumerate(self.records):
            # For each record, find the matching receipt
            # and update the corresponding timestamp.
            obj_cid = ds_record.get_cid()
            matches = [r["objectCid"] == obj_cid for r in commitment_receipts]
            i_match = next((i for i, v in enumerate(matches) if v), -1)
            if i_match == -1:
                l_log.append(
                    "Invalid record: "
                    "Failed to find timestamp for object: "
                    f"owner = {self.owner}, "
                    f"set_cid = {self.cid}, "
                    f"object_cid = {obj_cid}"
                )
                success = False
            else:
                self.timestamps[i] = commitment_receipts[i_match]["timestamp"]

        return success, l_log

    def get_pd_data_frame(self) -> Union[pd.DataFrame, None]:
        """
        Get a Pandas DataFrame representation of the dataset's records.
        This default method works for most datasets.
        Datasets that need special handling will override this method.

        :return: The pd.DataFrame object representing the dataset's records.
        """
        # TODO: Factor out the common code to get all simulation indices.
        if self.vbc.in_sim():
            t = self.vbc.get_sim_t()
            assert t is not None
            inds_match = np.where(np.array(self.timestamps) <= t.timestamp())[0]
            if len(inds_match) == 0:
                return None
        else:
            inds_match = np.arange(len(self.records))
        return pd.DataFrame(
            [self.records[i].get_dict() for i in inds_match],
            index=[pd.Timestamp(self.timestamps[i], tz="UTC") for i in inds_match],
        )
