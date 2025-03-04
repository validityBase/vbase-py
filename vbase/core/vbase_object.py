"""
Objects supported by the validityBase (vBase) platform.
A vBase objects are the basic digital objects
for which commitments and operations are supported.
Higher-order abstractions, such as datasets
comprise one or more records (objects) belonging to a set.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union

from vbase.utils.crypto_utils import float_to_field, hash_typed_values, string_to_u64_id
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class VBaseObject(ABC):
    """
    Provides basic Python vBase object features.
    Implements base functionality shared across various objects and dataset records.
    Children implement object-specific logic.
    """

    data: Any
    cid: Union[str, None]

    def __init__(
        self,
        init_data: Optional[Any] = None,
        init_dict: Optional[Dict] = None,
        init_json: Optional[str] = None,
    ):
        """
        Create an object.
        Can crate objects using a variety of inputs.
        Exactly one init argument below should be provided.

        :param init_data: Object raw data.
        :param init_dict: Object dictionary representation.
        :param init_json: Object JSON representation
        """
        self.cid = None

        # Verify that exactly one input is provided.
        inputs_provided = sum(x is not None for x in [init_data, init_dict, init_json])
        if inputs_provided != 1:
            raise ValueError(
                "Exactly one of init_data, init_dict, or init_json must be provided."
            )

        if init_dict is not None:
            self._init_from_dict(init_dict)
        elif init_json is not None:
            self._init_from_json(init_json)
        else:
            self.data = init_data
        self.cid = None

    @abstractmethod
    def _init_from_dict(self, init_dict: dict):
        """
        Initialize an object using a dictionary.

        :param init_dict: Object dictionary representation.
        """

    def _init_from_json(self, init_json: str):
        """
        Initialize an object using a JSON string.

        :param init_json: Object JSON representation
        """
        self._init_from_dict(json.loads(init_json))

    @staticmethod
    @abstractmethod
    def get_cid_for_data(record_data: Any) -> str:
        """
        Generate a content identifier (CID) for an object with given data.
        The method may be called to post commitments without instantiating an object.
        The encapsulation of different digital objects and
        their CID calculation is a primary job of an object.

        :param record_data: The object data.
            Allows calculating a CID without instantiating an object.
        :return: The CID generated.
        """

    def get_cid(self) -> str:
        """
        Return the content identifier (CID) for the object.
        Calculates the CID if necessary and caches it for subsequent queries.

        :return: The CID generated.
        """
        if self.cid is None:
            self.cid = self.get_cid_for_data(self.data)
        return self.cid

    def get_dict(self) -> dict:
        """
        Return the dictionary representation of the object's data.
        This is a basic implementation that most objects should override
        with more intelligent object-specific implementations.
        Converting objects to dictionaries is useful as a step in converting
        sets to DataFrames.

        :return: The dictionary representation of the object.
        """
        return {"data": self.data}


class VBaseIntObject(VBaseObject):
    """
    An integer object
    """

    def __init__(
        self,
        init_data: Optional[int] = None,
        init_dict: Optional[Dict[str, int]] = None,
        init_json: Optional[str] = None,
    ):
        super().__init__(init_data, init_dict, init_json)

    def _init_from_dict(self, init_dict: Dict[str, int]):
        self.data: int = int(init_dict["data"])

    @staticmethod
    def get_cid_for_data(record_data: int) -> str:
        return hash_typed_values(["uint256"], [record_data])


class VBasePrivateIntObject(VBaseObject):
    """
    An integer object that preserves object privacy
    Each object comprises an integer value and a string salt.
    The user-specified random salt preserves privacy of the data with low entropy.
    To verify the object, users must specify the preimage with salts.
    The source datasets to be validated will be commonly stored as a spreadsheet with two columns.
    """

    def __init__(
        self,
        init_data: Optional[Union[int, str]] = None,
        init_dict: Optional[Dict[str, Union[int, str]]] = None,
        init_json: Optional[str] = None,
    ):
        super().__init__(init_data, init_dict, init_json)

    def _init_from_dict(self, init_dict: Dict[str, Union[int, str]]):
        # The data tuple is converted to a list when saved as dictionary or JSON.
        # So we must load the tuple from the list.
        self.data: Tuple[int, str] = (
            int(init_dict["data"][0]),
            str(init_dict["data"][1]),
        )

    @staticmethod
    def get_cid_for_data(record_data: Tuple[int, str]) -> str:
        return hash_typed_values(
            ["uint256", "string"], [record_data[0], record_data[1]]
        )


class VBaseFloatObject(VBaseObject):
    """
    A float object
    Floats are committed as fixed-point integers to support ZKPs.
    """

    def __init__(
        self,
        init_data: Optional[float] = None,
        init_dict: Optional[Dict[str, float]] = None,
        init_json: Optional[str] = None,
    ):
        super().__init__(init_data, init_dict, init_json)

    def _init_from_dict(self, init_dict: Dict[str, float]):
        self.data: float = float(init_dict["data"])

    @staticmethod
    def get_cid_for_data(record_data: float) -> str:
        return hash_typed_values(["uint256"], [float_to_field(record_data)])


class VBasePrivateFloatObject(VBaseObject):
    """
    A float object that preserves object privacy
    Each object comprises a float value and a string salt.
    """

    def __init__(
        self,
        init_data: Optional[Union[float, str]] = None,
        init_dict: Optional[Dict[str, Union[float, str]]] = None,
        init_json: Optional[str] = None,
    ):
        super().__init__(init_data, init_dict, init_json)

    def _init_from_dict(self, init_dict: Dict[str, Union[float, str]]):
        # The data tuple is converted to a list when saved as dictionary or JSON.
        # So we must load the tuple from the list.
        self.data: Tuple[float, str] = (
            float(init_dict["data"][0]),
            str(init_dict["data"][1]),
        )

    @staticmethod
    def get_cid_for_data(record_data: Tuple[int, str]) -> str:
        return hash_typed_values(
            ["uint256", "string"],
            [float_to_field(record_data[0]), record_data[1]],
        )


class VBaseStringObject(VBaseObject):
    """
    A string object
    """

    def __init__(
        self,
        init_data: Optional[str] = None,
        init_dict: Optional[Dict[str, str]] = None,
        init_json: Optional[str] = None,
    ):
        super().__init__(init_data, init_dict, init_json)

    def _init_from_dict(self, init_dict: Dict[str, str]):
        self.data: str = str(init_dict["data"])

    @staticmethod
    def get_cid_for_data(record_data: str) -> str:
        return hash_typed_values(["string"], [record_data])


class VBaseJsonObject(VBaseObject):
    """
    A JSON string object
    """

    def __init__(
        self,
        init_data: Optional[str] = None,
        init_dict: Optional[Dict[str, str]] = None,
        init_json: Optional[str] = None,
    ):
        super().__init__(init_data, init_dict, init_json)

    def _init_from_dict(self, init_dict: Dict[str, str]):
        # When the JSON object is saved as a dictionary or JSON, data remains a string.
        # Se we can simply read it as a string.
        self.data: str = str(init_dict["data"])

    @staticmethod
    def get_cid_for_data(record_data: str) -> str:
        return VBaseStringObject.get_cid_for_data(record_data)

    def get_dict(self) -> dict:
        return json.loads(self.data)


class VBasePortfolioObject(VBaseObject):
    """
    A portfolio object
    Each portfolio is a dictionary with
    symbol/id keys and weight values.
    """

    def __init__(
        self,
        init_data: Optional[Dict[str, Union[int, float]]] = None,
        init_dict: Optional[Dict[str, Dict[str, Union[int, float]]]] = None,
        init_json: Optional[str] = None,
    ):
        super().__init__(init_data, init_dict, init_json)

    def _init_from_dict(self, init_dict: Dict[str, Dict[str, Union[int, float]]]):
        self.data: Dict[str, Union[int, float]] = init_dict["data"]

    @staticmethod
    def get_cid_for_data(record_data: dict) -> str:
        assert all(isinstance(key, str) for key in record_data.keys())
        assert all(isinstance(value, (int, float)) for value in record_data.values())
        # Portfolio symbols are mapped to u64 numeric values.
        # Truncate to 64 bits as ids are stored as u64 values.
        ids: List[Any] = [string_to_u64_id(key) for key in record_data.keys()]
        # Portfolio weights are mapped to field numeric values with
        # fixed point base -- uint256.
        vals: List[Any] = [float_to_field(val) for val in list(record_data.values())]
        # Both arrays are concatenated to calculate the hash.
        return hash_typed_values(
            ["uint64"] * len(ids) + ["uint256"] * len(vals),
            ids + vals,
        )


VBASE_OBJECT_TYPES = {
    "VBaseIntObject": VBaseIntObject,
    "VBasePrivateIntObject": VBasePrivateIntObject,
    "VBaseFloatObject": VBaseFloatObject,
    "VBasePrivateFloatObject": VBasePrivateFloatObject,
    "VBaseStringObject": VBaseStringObject,
    "VBaseJsonObject": VBaseJsonObject,
    "VBasePortfolioObject": VBasePortfolioObject,
}
