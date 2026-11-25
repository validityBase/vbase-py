"""Common count and offset pagination over distinct collection IDs."""

from dataclasses import dataclass

from sqlalchemy import Connection, Select, select
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.functions import count


@dataclass(frozen=True)
class CollectionCidPage:
    """One page of indexed collection CIDs and the total before pagination."""

    cids: list[str]
    total: int


def read_cid_page(
    conn: Connection, query: Select, order: ColumnElement, limit: int, offset: int
) -> CollectionCidPage:
    """The caller supplies one row per CID, already scoped to the user."""
    total = conn.execute(select(count()).select_from(query.subquery())).scalar_one()
    if not limit or offset >= total:
        return CollectionCidPage([], total)
    page = query.order_by(order).offset(offset).limit(min(limit, total))
    return CollectionCidPage(list(conn.execute(page).scalars()), total)
