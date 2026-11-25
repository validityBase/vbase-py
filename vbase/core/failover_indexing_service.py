"""Failover indexing service that tries multiple services in sequence."""

import logging
from collections.abc import Iterable
from threading import Lock
from typing import Any

from vbase.core.indexing_service import (
    CollectionCidPage,
    IndexingService,
    ObjectReceiptPage,
)
from vbase.utils.log import RedactUrlsFilter, get_default_logger, redact_exception

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)
# Indexing service errors can include credential-bearing URLs.
_LOG.addFilter(RedactUrlsFilter())


class FailoverIndexingService(IndexingService):  # pylint: disable=abstract-method
    """This indexing service calls a set of indexing services one after another
    and provides a failover mechanism to ensure that if one service fails,
    another service can be used to retrieve the data.

    Unpaged reads try each backend in order. Each paged method sticks with
    its first successful backend for the lifetime of this instance.
    """

    def __init__(self, services: list[IndexingService]):
        """Initialize the failover indexing service with a list of services."""
        self.services = services
        self._page_backends: dict[str, IndexingService] = {}
        self._selection_lock = Lock()

    def _execute_with_failover(self, method_name: str, *args, **kwargs):
        """Execute a method on the first available service that does not raise an exception."""
        return self._try_services(self.services, method_name, *args, **kwargs)[1]

    @staticmethod
    def _try_services(
        services: Iterable[IndexingService], method_name: str, *args, **kwargs
    ) -> tuple[IndexingService, Any]:
        last_error = None
        for service in services:
            try:
                return service, getattr(service, method_name)(*args, **kwargs)
            except Exception as error:  # pylint: disable=broad-exception-caught
                # Services other than Web3 and the forwarder may not redact
                # their errors; redact before the error is logged or chained.
                redact_exception(error)
                _LOG.error(
                    "Service %s failed to execute %s with error: %s: %s",
                    type(service).__name__,
                    method_name,
                    type(error).__name__,
                    error,
                )
                last_error = error

        # Chain the last service error, with URLs redacted, so callers can see
        # what failed.
        raise RuntimeError("All services failed to execute the method.") from last_error

    def _page(self, method: str, user: str, limit: int, offset: int) -> Any:
        with self._selection_lock:
            backend = self._page_backends.get(method)
            if backend is None:
                if offset:
                    raise RuntimeError(
                        "Start failover pagination at offset 0 on this instance"
                    )
                backend, page = self._try_services(
                    self.services, method, user, limit=limit, offset=offset
                )
                self._page_backends[method] = backend
                return page
        try:
            return self._try_services(
                (backend,), method, user, limit=limit, offset=offset
            )[1]
        except RuntimeError as exc:
            raise RuntimeError(
                "Paged backend unavailable; retry or restart at offset 0 on a new failover instance"
            ) from exc

    def find_user_sets(self, user: str) -> list[dict]:
        return self._execute_with_failover("find_user_sets", user)

    def _find_user_collection_cids(
        self, user: str, *, limit: int, offset: int
    ) -> CollectionCidPage:
        return self._page("find_user_collection_cids", user, limit, offset)

    def _find_user_objects_page(
        self, user: str, *, limit: int, offset: int
    ) -> ObjectReceiptPage:
        return self._page("find_user_objects_page", user, limit, offset)

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
