"""Session-local pagination over finalized Web3 events. No I/O on page reads."""

from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from operator import itemgetter
from threading import Lock

from vbase.core.indexing_page import ObjectReceiptPage, SetReceiptPage
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


class _HistoryMismatch(RuntimeError):
    """The provider returned conflicting evidence for finalized history."""


def _range_logs(
    cs: Web3HTTPCommitmentService,
    fetch: Callable,
    start: int,
    end: int,
    headers: dict,
) -> Iterator[tuple[str, dict, int]]:
    """Check returned logs; completeness of range responses is a provider guarantee."""
    seen: set[tuple[int, int]] = set()
    contract = cs.csc.address
    for name in ("AddObject", "AddSetObject"):
        decoder = getattr(cs.csc.events, name)
        params = {
            "address": contract,
            "topics": [decoder.topic],
            "fromBlock": start,
            "toBlock": end,
        }
        for raw in fetch(cs.w3.eth.get_logs, filter_params=params):
            # ContractEvent.get_logs() drops the provider's removal flag during ABI decoding.
            if raw.get("removed"):
                raise _HistoryMismatch("Removed log in finalized block history")
            number = raw["blockNumber"]
            identity = (number, raw["logIndex"])
            if (
                not start <= number <= end
                or raw["address"].lower() != contract.lower()
                or identity in seen
            ):
                raise _HistoryMismatch("Log does not match finalized block history")
            if number not in headers:
                headers[number] = cs.w3.eth.get_block(number)
            header = headers[number]
            if header["number"] != number or raw["blockHash"] != header["hash"]:
                raise _HistoryMismatch("Log does not match finalized block history")
            seen.add(identity)
            yield name, decoder.process_log(raw), header["timestamp"]


class Web3EventIndex:
    """Keep per-source progress and publish both views after all sources sync."""

    def __init__(self) -> None:
        self._session = _Session()
        self._sync_lock = Lock()
        self._invalid = False

    def _check_valid(self) -> None:
        if self._invalid:
            raise RuntimeError("Invalid Web3 history: start a new indexing session")

    def _pages(self) -> _Pages:
        self._check_valid()
        pages = self._session.pages
        if pages is None:
            raise RuntimeError("Session index not ready: run sync_index()")
        return pages

    def set_cids(self, user: str, limit: int, offset: int) -> SetReceiptPage:
        """Slice a sorted list of distinct CIDs without contacting the node."""
        rows = self._pages().cids.get(user.lower(), [])
        return SetReceiptPage(rows[offset : offset + limit], len(rows))

    def object_receipts(self, user: str, limit: int, offset: int) -> ObjectReceiptPage:
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
            self._check_valid()
            sources = {
                (cs.w3.eth.chain_id, cs.csc.address.lower()): cs for cs in services
            }
            if self._session.ranges and self._session.ranges.keys() != sources.keys():
                self._invalid = True
                raise ValueError("Web3 sources changed: start a new indexing session")
            if not sources:
                raise ValueError("At least one Web3 source is required")
            if not self._session.ranges:
                self._session.ranges = {key: [_Range()] for key in sources}
            try:
                for key, cs in sources.items():
                    self._sync_source(key, cs, fetch, batch_size)
            except _HistoryMismatch:
                self._invalid = True
                raise
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
        finalized = eth.get_block("finalized")
        target = finalized["number"]
        if target < height:
            raise RuntimeError("Node is behind the saved finalized checkpoint")
        if height >= 0 and (
            eth.get_block(height)["hash"] != last.block_hash
            or (height == target and finalized["hash"] != last.block_hash)
        ):
            raise _HistoryMismatch("Node does not match the saved finalized checkpoint")
        while height < target:
            end = min(height + batch_size, target)
            batch = self._read_range(cs, key[0], fetch, ranges[-1], end)
            if end == target and batch.block_hash != finalized["hash"]:
                raise _HistoryMismatch(
                    "Finalized history changed while reading a range"
                )
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
        previous: _Range,
        end: int,
    ) -> _Range:
        start = previous.end + 1
        get_block = cs.w3.eth.get_block
        headers = {end: get_block(end)}
        if headers[end]["number"] != end:
            raise _HistoryMismatch("Unexpected range end in finalized history")
        if previous.end >= 0:
            if start != end:
                headers[start] = get_block(start)
            if (
                headers[start]["number"] != start
                or headers[start]["parentHash"] != previous.block_hash
            ):
                raise _HistoryMismatch(
                    "Range does not extend the saved finalized checkpoint"
                )
        batch = _Range(end, headers[end]["hash"])
        for name, event, timestamp in _range_logs(cs, fetch, start, end, headers):
            args = event["args"]
            user = args["user"].lower()
            if name == "AddSetObject":
                batch.cids.add((user, bytes_to_hex_str_auto(args["setCid"]).lower()))
            else:
                batch.receipts.append(
                    {
                        "user": user,
                        "chainId": chain_id,
                        "transactionHash": bytes_to_hex_str_auto(
                            event["transactionHash"]
                        ).lower(),
                        "objectCid": bytes_to_hex_str_auto(args["objectCid"]).lower(),
                        "timestamp": cs.convert_timestamp_chain_to_str(timestamp),
                    }
                )
        if any(
            child["parentHash"] != headers[number - 1]["hash"]
            for number, child in headers.items()
            if number - 1 in headers
        ):
            raise _HistoryMismatch("Adjacent headers disagree on finalized history")
        if (
            previous.end >= 0
            and get_block(previous.end)["hash"] != previous.block_hash
            or get_block(end)["hash"] != batch.block_hash
        ):
            raise _HistoryMismatch("Finalized history changed while reading a range")
        return batch

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
