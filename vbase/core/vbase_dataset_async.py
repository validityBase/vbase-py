"""
Asynchronous dataset support for the validityBase (vBase) platform.
A vBase dataset comprises one or more records (objects) belonging to a set.
Asynchronous dataset wraps synchronous dataset object to support
async operations using asyncio.
"""

import asyncio
import logging
from typing import List, Union

import pandas as pd

from vbase.core.vbase_dataset import VBaseDataset
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class VBaseDatasetAsync(VBaseDataset):
    """
    Provides Python vBase dataset async access.
    Asynchronous dataset wraps synchronous dataset object to support
    async operations using asyncio.
    """

    @classmethod
    async def create(cls, *args, **kwargs) -> "VBaseDatasetAsync":
        """
        Creates a vBase dataset object asynchronously.
        A static async factory method that delegates to the synchronous constructor.
        Offloads VBaseDataset constructor execution to the default event loop's executor.

        :param args: Arguments passed to the VBaseDataset constructor.
        :param kwargs: Arguments passed to the VBaseDataset constructor.
        :return: The created dataset.
        """
        # Use run_in_executor to run the long-running sync __init__ in a separate thread.
        # This way, the caller can await the completion of the operation
        # without blocking the event loop.
        # A similar technique will be used for the other long-running blocking calls.
        loop = asyncio.get_event_loop()
        dataset = await loop.run_in_executor(None, lambda: cls(*args, **kwargs))
        return dataset

    async def add_record_async(self, record_data: any) -> dict:
        """
        Add a record to a VBase dataset object asynchronously.
        Offloads add_record execution to the default event loop's executor.

        :param record_data: The record datum.
        :return: The commitment log containing commitment receipt info.
        """
        loop = asyncio.get_event_loop()
        cl = await loop.run_in_executor(None, self.add_record, record_data)
        return cl

    async def add_records_batch_async(self, record_data_list: List[any]) -> List[dict]:
        """
        Add a record to a VBase dataset object asynchronously.
        Offloads add_record execution to the default event loop's executor.

        :param record_data_list: The list of records' data.
        :return: The commitment log list containing commitment receipts.
        """
        loop = asyncio.get_event_loop()
        cls = await loop.run_in_executor(None, self.add_records_batch, record_data_list)
        return cls

    async def add_record_with_timestamp_async(
        self, record_data: any, timestamp: Union[pd.Timestamp, str]
    ) -> dict:
        """
        Test shim to add a record to a VBaseDataset object
        with a given timestamp asynchronously.
        Only supported by test contracts.
        Offloads add_record_with_timestamp execution
        to the default event loop's executor.

        :param record_data: The record datum.
        :param timestamp: Timestamp to force for the record.
        :return: The commitment log containing commitment receipt info.
        """
        loop = asyncio.get_event_loop()
        cls = await loop.run_in_executor(
            None, self.add_record_with_timestamp, record_data, timestamp
        )
        return cls

    async def add_records_with_timestamps_batch_async(
        self,
        record_data_list: List[any],
        timestamps: List[Union[pd.Timestamp, str]],
    ) -> List[dict]:
        """
        Test shim to add a batch of records with timestamps
        to a VBaseDataset object asynchronously.
        Only supported by test contracts.
        Offloads add_records_with_timestamps_batch execution
        to the default event loop's executor.

        :param record_data_list: The list of records' data.
        :param timestamps: The list of timestamps to force for the records.
        :return: The commitment log list containing commitment receipts.
        """
        loop = asyncio.get_event_loop()
        cls = await loop.run_in_executor(
            None, self.add_records_with_timestamps_batch, record_data_list, timestamps
        )
        return cls

    async def verify_commitments_async(self) -> (bool, List[str]):
        """
        Verify commitments for all dataset records asynchronously.
        Offloads verify_commitments execution
        to the default event loop's executor.

        :return: A tuple containing success and log:
            - success: true if all record commitments have been verified; false otherwise
            - l_log: a list log of verification explaining any failures
        """
        loop = asyncio.get_event_loop()
        cls = await loop.run_in_executor(None, self.verify_commitments)
        return cls
