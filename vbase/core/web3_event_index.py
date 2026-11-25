"""Session-local pagination over finalized Web3 events. No I/O on page reads."""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from operator import itemgetter
from threading import Lock

from vbase.core.indexing_page import CollectionCidPage, ObjectReceiptPage
from vbase.core.web3_http_commitment_service import Web3HTTPCommitmentService
from vbase.utils.crypto_utils import bytes_to_hex_str_auto


@dataclass
class _Pages:
    """Published lists are never mutated, so readers can retain one complete view."""

    cids: dict[str, list[str]] = field(default_factory=dict)
    receipts: dict[str, list[dict]] = field(default_factory=dict)


@dataclass
class _Range:
    """The checkpoint travels with the events it covers, even on cancellation."""

    end: int = -1
    block_hash: bytes = b""
    receipts: list[dict] = field(default_factory=list)
    cids: set[tuple[str, str]] = field(default_factory=set)


@dataclass
class _Session:
    pages: _Pages | None = None
    ranges: dict[tuple[int, str], list[_Range]] = field(default_factory=dict)


def _stamp_receipts(
    cs: Web3HTTPCommitmentService, chain_id: int, events: list, times: dict[int, int]
) -> list[dict]:
    """Use the hosted indexer's block time, not the contract payload timestamp."""
    receipts = []
    for event in events:
        block = event["blockNumber"]
        if block not in times:
            times[block] = cs.w3.eth.get_block(block)["timestamp"]
        receipts.append(
            {
                "user": event["args"]["user"].lower(),
                "chainId": chain_id,
                "transactionHash": bytes_to_hex_str_auto(
                    event["transactionHash"]
                ).lower(),
                "objectCid": bytes_to_hex_str_auto(event["args"]["objectCid"]).lower(),
                "timestamp": cs.convert_timestamp_chain_to_str(times[block]),
            }
        )
    return receipts


class Web3EventIndex:
    """Keep per-source progress and publish both views after all sources sync."""

    def __init__(self) -> None:
        self._session = _Session()
        self._sync_lock = Lock()

    def _pages(self) -> _Pages:
        pages = self._session.pages
        if pages is None:
            raise RuntimeError("Session index not ready: run sync_index()")
        return pages

    def collections(self, user: str, limit: int, offset: int) -> CollectionCidPage:
        """Slice a sorted list of distinct CIDs without contacting the node."""
        rows = self._pages().cids.get(user.lower(), [])
        return CollectionCidPage(rows[offset : offset + limit], len(rows))

    def stamps(self, user: str, limit: int, offset: int) -> ObjectReceiptPage:
        """Copy only the requested receipts; callers must not mutate the index."""
        rows = self._pages().receipts.get(user.lower(), [])
        return ObjectReceiptPage(
            [r.copy() for r in rows[offset : offset + limit]], len(rows)
        )

    def sync(
        self,
        services: list[Web3HTTPCommitmentService],
        fetch: Callable,
        batch_size: int,
    ) -> None:
        """Retry from the last complete range in this instance, including empty ranges."""
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size <= 0
        ):
            raise ValueError("block_batch_size must be a positive integer")
        with self._sync_lock:
            sources = {
                (cs.w3.eth.chain_id, cs.commitment_service_address.lower()): cs
                for cs in services
            }
            if not sources:
                raise ValueError("At least one Web3 source is required")
            if not self._session.ranges:
                self._session.ranges = {key: [_Range()] for key in sources}
            if self._session.ranges.keys() != sources.keys():
                raise ValueError("Web3 sources changed: start a new indexing session")
            for key, cs in sources.items():
                self._sync_source(key, cs, fetch, batch_size)
            self._publish()

    def _sync_source(
        self,
        key: tuple[int, str],
        cs: Web3HTTPCommitmentService,
        fetch: Callable,
        batch_size: int,
    ) -> None:
        ranges = self._session.ranges[key]
        last = ranges[-1]
        height = last.end
        eth = cs.w3.eth
        target = eth.get_block("finalized")["number"]
        if target < height or (
            height >= 0 and eth.get_block(height)["hash"] != last.block_hash
        ):
            raise RuntimeError("Node does not match the saved finalized checkpoint")
        while height < target:
            end = min(height + batch_size, target)
            batch = self._read_range(cs, key[0], fetch, height + 1, end)
            if ranges[-1].receipts or ranges[-1].cids:
                ranges.append(batch)
            else:
                ranges[-1] = batch
            height = end

    @staticmethod
    def _read_range(
        cs: Web3HTTPCommitmentService,
        chain_id: int,
        fetch: Callable,
        start: int,
        end: int,
    ) -> _Range:
        header = cs.w3.eth.get_block(end)
        block_hash = header["hash"]
        filters = {"from_block": start, "to_block": end, "argument_filters": {}}
        receipts = _stamp_receipts(
            cs,
            chain_id,
            fetch(cs.csc.events.AddObject, **filters),
            {end: header["timestamp"]},
        )
        cids = {
            (
                e["args"]["user"].lower(),
                bytes_to_hex_str_auto(e["args"]["setCid"]).lower(),
            )
            for e in fetch(cs.csc.events.AddSetObject, **filters)
        }
        return _Range(end, block_hash, receipts, cids)

    def _publish(self) -> None:
        old = self._session.pages or _Pages()
        new_cids: defaultdict[str, set[str]] = defaultdict(set)
        new_receipts: defaultdict[str, list[dict]] = defaultdict(list)
        checkpoints = {}
        for key, ranges in self._session.ranges.items():
            checkpoints[key] = [_Range(ranges[-1].end, ranges[-1].block_hash)]
            for batch in ranges:
                for receipt in batch.receipts:
                    new_receipts[receipt["user"]].append(receipt)
                for user, cid in batch.cids:
                    new_cids[user].add(cid)
        pages = old
        if new_receipts or new_cids:
            pages = _Pages(old.cids.copy(), old.receipts.copy())
            for user, cids in new_cids.items():
                pages.cids[user] = sorted(cids.union(old.cids.get(user, [])))
            order = itemgetter("timestamp", "chainId", "transactionHash", "objectCid")
            for user, receipts in new_receipts.items():
                pages.receipts[user] = sorted(
                    old.receipts.get(user, []) + receipts, key=order
                )
        # Publish both views and retire their pending batches in one assignment.
        self._session = _Session(pages, checkpoints)
