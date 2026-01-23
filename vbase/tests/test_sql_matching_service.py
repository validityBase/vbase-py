import datetime
import unittest
from typing import Union

import pandas as pd
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from vbase.core.sql_indexing_service import ObjectAtTime, event_add_set_object
from vbase.core.strategies import (
    SetMatchingCriteria,
    SetMatchingStrategy,
    SetMatchingStrategyConfig,
)


def to_unix_timestamp(ts: Union[int, str, datetime.datetime]) -> int:
    """
    Convert timestamp input to Unix timestamp (seconds, UTC).

    Accepts:
    - int: assumed already Unix seconds
    - datetime.datetime: must be timezone-aware
    - str: ISO-8601 string, e.g. '2024-01-01 12:00:00+00:00'
    """
    if isinstance(ts, int):
        return ts

    if isinstance(ts, datetime.datetime):
        if ts.tzinfo is None:
            raise ValueError("datetime must be timezone-aware")
        return int(ts.astimezone(datetime.timezone.utc).timestamp())

    if isinstance(ts, str):
        s = ts.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        try:
            dt = datetime.datetime.fromisoformat(s)
        except ValueError as e:
            raise ValueError(f"Invalid timestamp string: {ts!r}") from e

        if dt.tzinfo is None:
            raise ValueError("timestamp string must include timezone info")

        return int(dt.astimezone(datetime.timezone.utc).timestamp())

    raise TypeError(f"Unsupported timestamp type: {type(ts)}")


DAY = 24 * 60 * 60  # 24 hours in seconds
T0 = "2024-01-01 12:00:00+00:00"


def assert_matches(results, expected):
    actual = {(r.user, r.set_cid) for r in results}
    assert actual == expected


class TestSetMatchingStrategy(unittest.TestCase):
    def setUp(self):
        db_url = "sqlite:///file::memory:?cache=shared"
        self.engine = create_engine(db_url)
        SQLModel.metadata.drop_all(self.engine)
        SQLModel.metadata.create_all(self.engine)
        self.service = SetMatchingStrategy(
            self.engine,
            config=SetMatchingStrategyConfig(max_timestamp_diff=pd.Timedelta(days=1)),
        )

    def _insert_data(self, data):
        with Session(self.engine) as session:
            for item in data:
                session.add(
                    event_add_set_object(
                        id=item["id"],
                        user=item["user"],
                        set_cid=item["set_cid"],
                        object_cid=item["object_cid"],
                        timestamp=to_unix_timestamp(item["timestamp"]),
                        chain_id=item.get("chain_id", 1),
                        transaction_hash=item.get("transaction_hash", "0x123"),
                    )
                )
            session.commit()

    # ------------------------------------------------------------
    # 1. Same number of records (N ↔ N)
    # ------------------------------------------------------------
    def test_same_number_of_records(self):
        self._insert_data(
            [
                {
                    "id": "1",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o1",
                    "timestamp": T0,
                },
            ]
        )

        results = self.service.find_matching_user_sets(
            SetMatchingCriteria(
                objects=[ObjectAtTime("o1", to_unix_timestamp(T0))],
                as_of=pd.Timestamp(T0),
            )
        )

        assert_matches(results, {("u1", "s1")})

    # ------------------------------------------------------------
    # 2. One extra record (N ↔ N+1)
    # ------------------------------------------------------------
    def test_plus_one_record(self):
        self._insert_data(
            [
                {
                    "id": "1",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o1",
                    "timestamp": T0,
                },
                {
                    "id": "2",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o2",
                    "timestamp": "2024-01-02 12:00:00+00:00",
                },
            ]
        )

        results = self.service.find_matching_user_sets(
            SetMatchingCriteria(
                objects=[ObjectAtTime("o1", to_unix_timestamp(T0))],
                as_of=pd.Timestamp("2024-01-03 00:00:00+00:00"),
            )
        )

        assert_matches(results, {("u1", "s1")})

    # ------------------------------------------------------------
    # 3. One missing record (N ↔ N−1)
    # ------------------------------------------------------------
    def test_minus_one_record(self):
        self._insert_data(
            [
                {
                    "id": "1",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o1",
                    "timestamp": T0,
                },
            ]
        )

        results = self.service.find_matching_user_sets(
            SetMatchingCriteria(
                objects=[
                    ObjectAtTime("o1", to_unix_timestamp(T0)),
                    ObjectAtTime("o2", to_unix_timestamp(T0)),
                ],
                as_of=pd.Timestamp(T0),
            )
        )

        assert_matches(results, {("u1", "s1")})

    # ------------------------------------------------------------
    # 4. Single user, timestamp drift, multiple sets
    # ------------------------------------------------------------
    def test_single_user_timestamp_drift_multiple_sets(self):
        self._insert_data(
            [
                {
                    "id": "1",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o1",
                    "timestamp": "2024-01-02 10:00:00+00:00",
                },
                {
                    "id": "2",
                    "user": "u1",
                    "set_cid": "s2",
                    "object_cid": "o1",
                    "timestamp": "2023-12-31 14:00:00+00:00",
                },
            ]
        )

        results = self.service.find_matching_user_sets(
            SetMatchingCriteria(
                objects=[ObjectAtTime("o1", to_unix_timestamp(T0))],
                as_of=pd.Timestamp(T0),
            )
        )

        assert_matches(results, {("u1", "s2")})

    # ------------------------------------------------------------
    # 5. Multiple users, timestamp drift, multiple sets
    # ------------------------------------------------------------
    def test_multiple_users_timestamp_drift_multiple_sets(self):
        self._insert_data(
            [
                {
                    "id": "1",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o1",
                    "timestamp": "2024-01-02 10:00:00+00:00",
                },
                {
                    "id": "2",
                    "user": "u2",
                    "set_cid": "s2",
                    "object_cid": "o1",
                    "timestamp": "2023-12-31 14:00:00+00:00",
                },
            ]
        )

        results = self.service.find_matching_user_sets(
            SetMatchingCriteria(
                objects=[ObjectAtTime("o1", to_unix_timestamp(T0))],
                as_of=pd.Timestamp(T0),
            )
        )

        assert_matches(results, {("u2", "s2")})

    # ------------------------------------------------------------
    # 6. Multiple users, drift, multiple sets, different counts
    # ------------------------------------------------------------
    def test_multiple_users_multiple_sets_different_counts(self):
        self._insert_data(
            [
                {
                    "id": "1",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o1",
                    "timestamp": "2024-01-02 10:00:00+00:00",
                },
                {
                    "id": "2",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o2",
                    "timestamp": "2024-01-05 12:00:00+00:00",
                },
                {
                    "id": "3",
                    "user": "u2",
                    "set_cid": "s2",
                    "object_cid": "o1",
                    "timestamp": "2023-12-31 14:00:00+00:00",
                },
                {
                    "id": "4",
                    "user": "u2",
                    "set_cid": "s2",
                    "object_cid": "o3",
                    "timestamp": "2024-01-10 12:00:00+00:00",
                },
            ]
        )

        results = self.service.find_matching_user_sets(
            SetMatchingCriteria(
                objects=[ObjectAtTime("o1", to_unix_timestamp(T0))],
                as_of=pd.Timestamp(T0),
            )
        )

        assert_matches(results, {("u2", "s2")})

    # ------------------------------------------------------------
    # 7. as_of filters out future records
    # ------------------------------------------------------------
    def test_as_of_filters_future_records(self):
        self._insert_data(
            [
                {
                    "id": "1",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o1",
                    "timestamp": T0,
                },
                {
                    "id": "2",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o2",
                    "timestamp": "2024-01-10 12:00:00+00:00",
                },
            ]
        )

        results = self.service.find_matching_user_sets(
            SetMatchingCriteria(
                objects=[ObjectAtTime("o1", to_unix_timestamp(T0))],
                as_of=pd.Timestamp("2024-01-02 00:00:00+00:00"),
            )
        )

        assert_matches(results, {("u1", "s1")})

    # ------------------------------------------------------------
    # 8. Not found
    # ------------------------------------------------------------
    def test_not_found(self):
        self._insert_data(
            [
                {
                    "id": "1",
                    "user": "u1",
                    "set_cid": "s1",
                    "object_cid": "o2",
                    "timestamp": T0,
                },
            ]
        )

        results = self.service.find_matching_user_sets(
            SetMatchingCriteria(
                objects=[ObjectAtTime("o1", to_unix_timestamp(T0))],
                as_of=pd.Timestamp(T0),
            )
        )

        assert results == []


if __name__ == "__main__":
    unittest.main()
