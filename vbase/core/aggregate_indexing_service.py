import concurrent.futures
import logging

from vbase.core.indexing_service import IndexingService
from vbase.utils.log import get_default_logger

_LOG = get_default_logger(__name__)
_LOG.setLevel(logging.INFO)


class AggregateIndexingService(IndexingService):
    """This indexing service aggregates the responses from a set of indexing services

    Each operation executes a corresponding method on all services and aggregates the results.
    """

    def __init__(self, services: list[IndexingService]):
        """Initialize the aggregate indexing service with a list of services."""
        self.services = services

    def _execute_with_aggregation(self, method_name: str, *args, **kwargs):
        """Execute a method on all services and aggregate the results."""
        results = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(getattr(service, method_name), *args, **kwargs)
                for service in self.services
            ]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
        # Preserve order of services
        results_sorted = [None] * len(self.services)
        for idx, future in enumerate(futures):
            results_sorted[idx] = future.result()
        return results_sorted

    def _aggregate_by_transaction_hash(
        self, all_results: list[list[dict]]
    ) -> list[dict]:
        """Merge lists of dicts, removing duplicates by transactionHash."""
        merged = []
        seen_hashes = set()
        for service_result in all_results:
            for entry in service_result:
                tx_hash = entry.get("transactionHash")
                if tx_hash not in seen_hashes:
                    merged.append(entry)
                    seen_hashes.add(tx_hash)
        return merged

    def find_user_sets(self, user: str) -> list[dict]:
        """Aggregate user sets from all services, removing duplicates by transactionHash."""
        all_results = self._execute_with_aggregation("find_user_sets", user)
        return self._aggregate_by_transaction_hash(all_results)

    def find_user_objects(self, user: str, return_set_cids=False) -> list[dict]:
        """Aggregate user objects from all services, removing duplicates by transactionHash."""
        all_results = self._execute_with_aggregation(
            "find_user_objects", user, return_set_cids=return_set_cids
        )
        return self._aggregate_by_transaction_hash(all_results)

    def find_user_set_objects(self, user: str, set_cid: str) -> list[dict]:
        """Aggregate user set objects from all services, removing duplicates by transactionHash."""
        all_results = self._execute_with_aggregation(
            "find_user_set_objects", user, set_cid
        )
        return self._aggregate_by_transaction_hash(all_results)

    def _get_latest_by_timestamp(
        self, all_results: list, timestamp_key: str = "timestamp"
    ) -> dict | None:
        """Return the object with the latest timestamp from a list of results."""
        valid_results = [res for res in all_results if res is not None]
        if not valid_results:
            return None
        latest = max(valid_results, key=lambda x: x.get(timestamp_key, 0))
        return latest

    def find_last_user_set_object(self, user: str, set_cid: str) -> dict | None:
        """Aggregate the last user set object from all services, returning the one with the latest timestamp."""
        all_results = self._execute_with_aggregation(
            "find_last_user_set_object", user, set_cid
        )
        return self._get_latest_by_timestamp(all_results)

    def find_objects(self, object_cids: list[str], return_set_cids=False) -> list[dict]:
        """Aggregate objects from all services, removing duplicates by transactionHash."""
        all_results = self._execute_with_aggregation(
            "find_objects", object_cids, return_set_cids=return_set_cids
        )
        return self._aggregate_by_transaction_hash(all_results)

    def find_object(self, object_cid: str, return_set_cids=False) -> list[dict]:
        """Return the first non-empty list of objects found by the underlying services."""
        all_results = self._execute_with_aggregation(
            "find_object", object_cid, return_set_cids=return_set_cids
        )
        for result in all_results:
            if result:
                return result
        return []  # Return an empty list if no results found

    def find_last_object(self, object_cid: str, return_set_cid=False) -> dict | None:
        """Aggregate the last object from all services, returning the one with the latest timestamp."""
        all_results = self._execute_with_aggregation(
            "find_last_object", object_cid, return_set_cid=return_set_cid
        )
        return self._get_latest_by_timestamp(all_results)
