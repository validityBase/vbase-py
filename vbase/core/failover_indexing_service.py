"""Failover indexing service that tries multiple services in sequence."""

import logging

from vbase.core.indexing_service import IndexingService
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class FailoverIndexingService(IndexingService):
    """This indexing service calls a set of indexing services one after another
    and provides a failover mechanism to ensure that if one service fails,
    another service can be used to retrieve the data.

    Each operation will try to use the first service in the list.
    If it fails, it will try the next service in the list until it finds one that works.
    """

    def __init__(self, services: list[IndexingService]):
        """Initialize the failover indexing service with a list of services."""
        self.services = services

    def _execute_with_failover(self, method_name: str, *args, **kwargs):
        """Execute a method on the first available service that does not raise an exception."""
        for service in self.services:
            try:
                return getattr(service, method_name)(*args, **kwargs)
            except Exception as e:
                _LOG.error(f"Service {service} failed with error: {e}")

        raise Exception("All services failed to execute the method.")

    def find_user_sets(self, user: str) -> list[dict]:
        return self._execute_with_failover("find_user_sets", user)

    def find_user_objects(self, user: str, return_set_cids=False) -> list[dict]:
        return self._execute_with_failover(
            "find_user_objects", user, return_set_cids=return_set_cids
        )

    def find_user_set_objects(self, user: str, set_cid: str) -> dict:
        return self._execute_with_failover("find_user_set_objects", user, set_cid)

    def find_last_user_set_object(self, user: str, set_cid: str) -> dict:
        return self._execute_with_failover("find_last_user_set_object", user, set_cid)

    def find_objects(self, object_cids: list[str], return_set_cids=False) -> list[dict]:
        return self._execute_with_failover(
            "find_objects", object_cids, return_set_cids=return_set_cids
        )

    def find_object(self, object_cid: str, return_set_cids=False) -> dict:
        return self._execute_with_failover(
            "find_object", object_cid, return_set_cids=return_set_cids
        )

    def find_last_object(self, object_cid: str, return_set_cid=False) -> dict:
        return self._execute_with_failover(
            "find_last_object", object_cid, return_set_cid=return_set_cid
        )
