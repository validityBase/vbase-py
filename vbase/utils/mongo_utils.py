"""
Common utilities for MongoDB access
"""

import logging
import os
from typing import Union
from copy import deepcopy
from dotenv import load_dotenv
from pymongo import MongoClient

from vbase.utils.log import get_default_logger


_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class MongoUtils:
    """
    vBase MongoDB client wrapper
    Abstracts common vBase data availability layer operations.
    """

    def __init__(self, dotenv_path: Union[str, None] = None):
        """
        Initialize a client object.

        dotenv_path: Absolute or relative path to .env file.
        """
        # Load .env file if it exists.
        load_dotenv(dotenv_path, verbose=True, override=True)
        self.mongodb_url = os.getenv("MONGODB_URL")
        assert self.mongodb_url is not None
        _LOG.info("Using MongoDB URL: %s", self.mongodb_url)
        self.mongo_client = MongoClient(self.mongodb_url)
        self.col_datasets = None

    def get_commitment_service_addr(
        self,
        network: str = "localhost",
        commitment_service_contract_name: str = "CommitmentServiceTest",
    ) -> str:
        """
        Get the CommitmentService contract address from MongoDB.

        :param network: The CommitmentService contract network name.
        :param commitment_service_contract_name: The CommitmentService contract class name.
        :return: The CommitmentService contract address.
        """
        docs = list(
            self.mongo_client.get_database("vbase")
            .get_collection("addresses")
            .find({"network": network, "name": commitment_service_contract_name})
        )
        assert len(docs) == 1
        comm_addr = docs[0]["address"]
        _LOG.info(
            "Using %s at address: %s", commitment_service_contract_name, comm_addr
        )
        return comm_addr

    def _init_col_datasets_if_necessary(self):
        """
        Lazily init the datasets collection if necessary.
        """
        if self.col_datasets is None:
            self.col_datasets = self.mongo_client.get_database("vbase").get_collection(
                "datasets"
            )

    @staticmethod
    def get_dict_ds_filter(dict_ds: dict) -> dict:
        """
        Get filter for a dataset.

        :param dict_ds: The dictionary representation of the dataset.
        :return: The dictionary with the filter fields of the dataset
            that uniquely identify it.
        """
        return {k: dict_ds[k] for k in ["name", "owner", "hash"]}

    def write_ds_dict(self, dict_ds: dict):
        """
        Write a dataset to MongoDB.

        :param dict_ds: The dictionary representation of the dataset.
        """
        dict_ds_filter = {k: dict_ds[k] for k in ["name", "owner", "hash"]}
        self._init_col_datasets_if_necessary()
        if any(self.col_datasets.find(dict_ds_filter)):
            # PyMongo can modify the dict on write to add _id.
            # Use deepcopy().
            res = self.col_datasets.replace_one(dict_ds_filter, deepcopy(dict_ds))
            assert res.matched_count == 1
            assert res.modified_count == 1
        else:
            res = self.col_datasets.insert_one(deepcopy(dict_ds))
            assert res.acknowledged

    def read_ds_dict(self, dict_ds_filter: dict) -> dict:
        """
        Write a dataset to MongoDB.

        :param dict_ds_filter: The dictionary filter for the dataset.
        """
        self._init_col_datasets_if_necessary()
        l_ds = list(self.col_datasets.find(dict_ds_filter))
        assert len(l_ds) == 1
        dict_dsr = l_ds[0]
        del dict_dsr["_id"]
        return dict_dsr
