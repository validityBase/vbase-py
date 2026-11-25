"""Page contracts for indexed blockchain events."""

from dataclasses import dataclass


@dataclass(frozen=True)
class CollectionCidPage:
    """Distinct collection CIDs and their count before pagination."""

    cids: list[str]
    total: int


@dataclass(frozen=True)
class ObjectReceiptPage:
    """Individual object stamp receipts and their count before pagination."""

    receipts: list[dict]
    total: int


def validate_page(limit: int, offset: int) -> None:
    """Only non-negative integers are valid page arguments."""
    for name, value in (("limit", limit), ("offset", offset)):
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
