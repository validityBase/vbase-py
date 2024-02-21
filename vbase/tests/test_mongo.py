"""
Tests of the MongoDB interface
MongoDB is used for name resolution and temporary storage during tests.
"""

import json
import unittest

from vbase.utils.mongo_utils import MongoUtils


class TestMongo(unittest.TestCase):
    """
    Test mongo utilities and install.
    """

    def setUp(self):
        """
        Set up the tests.
        """
        self.mongo_utils = MongoUtils()

    def test_ds_wr(self):
        """
        Test dataset write and read.
        """
        dict_dsw = {
            "name": "TestDataset",
            "owner": "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
            "hash": "0x0bc3e9d9197622a469b05f31ae374efacda8e98d45dc69d8c0ec7537468aabbc",
            "records": [
                {"AAA": 0.2, "BBB": 0.31, "CCC": 0.49},
                {"AAA": 0.2, "BBB": 0.32, "CCC": 0.48},
                {"AAA": 0.2, "BBB": 0.33, "CCC": 0.47},
                {"AAA": 0.2, "BBB": 0.34, "CCC": 0.46},
                {"AAA": 0.2, "BBB": 0.35, "CCC": 0.45},
            ],
            "timestamps": [1, 2, 3, 4, 5],
        }
        self.mongo_utils.write_ds_dict(dict_dsw)
        dict_ds_filter = self.mongo_utils.get_dict_ds_filter(dict_dsw)
        dict_dsr = self.mongo_utils.read_ds_dict(dict_ds_filter)
        assert json.dumps(dict_dsw, sort_keys=True) == json.dumps(
            dict_dsr, sort_keys=True
        )
