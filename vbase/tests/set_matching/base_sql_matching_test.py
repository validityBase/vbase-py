"""
Abstract base class for testing SQL-based set matching services.
"""

import os
import tempfile
import unittest
from abc import ABC

from sqlmodel import Session, SQLModel, create_engine

from vbase.core.models import EventAddSetObject, LastBatchProcessingTime


class BaseSQLMatchingTest(unittest.TestCase, ABC):
    """
    Abstract base class for unit tests of set matching services.

    Provides:
    - Temporary file-backed SQLite database setup and teardown
    - Helper method to add test EventAddSetObject records
    - Database URL for passing to matching service constructors
    """

    def setUp(self) -> None:
        """Create a temporary file-backed SQLite database and initialize schema."""
        super().setUp()
        self.db_url, self.db_engine, self.db_file = self.create_db()

    def tearDown(self) -> None:
        """Clean up database resources."""
        super().tearDown()
        self.db_engine.dispose()
        # Remove temp file if it exists
        if self.db_file and os.path.exists(self.db_file):
            try:
                os.unlink(self.db_file)
            except OSError:
                pass  # Best effort cleanup

    @staticmethod
    def create_db() -> tuple[str, object, str | None]:
        """
        Create a temporary SQLite database for testing.

        Returns:
            tuple: (db_url, db_engine, db_file) - URL string, SQLAlchemy engine instance,
                   and temp file path. The db_url can be passed to matching service constructors.
        """
        # Create a temporary file for the database
        fd, db_file = tempfile.mkstemp(suffix=".db")
        os.close(fd)  # Close the file descriptor, SQLite will open it

        db_url = f"sqlite:///{db_file}"
        engine = create_engine(db_url)

        # Create all tables defined in SQLModel (including EventAddSetObject)
        SQLModel.metadata.create_all(engine)

        return db_url, engine, db_file

    def add_test_event(
        self,
        event_id: str,
        user: str,
        set_cid: str,
        object_cid: str,
        chain_id: int,
        timestamp: int,
        transaction_hash: str = "0x0",
    ) -> None:
        """
        Add a single EventAddSetObject record to the test database.

        Args:
            event_id: Unique identifier for this event
            user: User address (e.g., "0xABC...")
            set_cid: Content ID of the set
            object_cid: Content ID of the object
            chain_id: Blockchain chain ID (e.g., 1 for mainnet)
            timestamp: Unix timestamp
            transaction_hash: Transaction hash (defaults to "0x0")
        """
        event = EventAddSetObject(
            id=event_id,
            user=user,
            set_cid=set_cid,
            object_cid=object_cid,
            chain_id=chain_id,
            transaction_hash=transaction_hash,
            timestamp=timestamp,
        )

        with Session(self.db_engine) as session:
            session.add(event)
            session.commit()

    def add_last_batch_processing_time(
        self, timestamp: int, record_id: str = "batch-1"
    ) -> None:
        """Add a LastBatchProcessingTime record to the test database."""
        record = LastBatchProcessingTime(id=record_id, timestamp=timestamp)
        with Session(self.db_engine) as session:
            session.add(record)
            session.commit()

    def add_test_events(self, events: list[dict]) -> None:
        """
        Add multiple EventAddSetObject records to the test database.

        Args:
            events: List of dictionaries, each containing event fields:
                   - id: str
                   - user: str
                   - set_cid: str
                   - object_cid: str
                   - chain_id: int
                   - timestamp: int
                   - transaction_hash: str (optional, defaults to "0x0")
        """
        event_objects = [
            EventAddSetObject(
                id=e["id"],
                user=e["user"],
                set_cid=e["set_cid"],
                object_cid=e["object_cid"],
                chain_id=e["chain_id"],
                transaction_hash=e.get("transaction_hash", "0x0"),
                timestamp=e["timestamp"],
            )
            for e in events
        ]

        with Session(self.db_engine) as session:
            for event in event_objects:
                session.add(event)
            session.commit()
