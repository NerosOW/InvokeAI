from abc import ABC, abstractmethod
from typing import cast
import uuid
import sqlite3
from typing import (
    List,
    Literal,
    Optional,
    Union,
)
import json

from invokeai.app.invocations.baseinvocation import (
    BaseInvocation,
)
from invokeai.app.services.thread import SqliteLock
from invokeai.app.services.graph import Graph
from invokeai.app.models.image import ImageField

from pydantic import BaseModel, Field, Extra, StrictFloat, StrictInt, StrictStr, parse_raw_as

invocations = BaseInvocation.get_invocations()
InvocationsUnion = Union[invocations]  # type: ignore

BatchDataType = Union[StrictStr, StrictInt, StrictFloat, ImageField]


class Batch(BaseModel):
    data: list[dict[str, BatchDataType]] = Field(description="Mapping of node field to data value")
    node_id: str = Field(description="ID of the node to batch")


class BatchSession(BaseModel):
    batch_id: str = Field(description="Identifier for which batch this Index belongs to")
    session_id: str = Field(description="Session ID Created for this Batch Index")
    state: Literal["created", "completed", "inprogress", "error"] = Field(
        description="Is this session created, completed, in progress, or errored?"
    )


def uuid_string():
    res = uuid.uuid4()
    return str(res)


class BatchProcess(BaseModel):
    batch_id: Optional[str] = Field(default_factory=uuid_string, description="Identifier for this batch")
    batches: List[Batch] = Field(
        description="List of batch configs to apply to this session",
        default_factory=list,
    )
    canceled: bool = Field(description="Flag for saying whether or not to run sessions from this batch", default=False)
    graph: Graph = Field(description="The graph being executed")


class BatchSessionChanges(BaseModel, extra=Extra.forbid):
    state: Literal["created", "completed", "inprogress", "error"] = Field(
        description="Is this session created, completed, in progress, or errored?"
    )


class BatchProcessNotFoundException(Exception):
    """Raised when an Batch Process record is not found."""

    def __init__(self, message="BatchProcess record not found"):
        super().__init__(message)


class BatchProcessSaveException(Exception):
    """Raised when an Batch Process record cannot be saved."""

    def __init__(self, message="BatchProcess record not saved"):
        super().__init__(message)


class BatchProcessDeleteException(Exception):
    """Raised when an Batch Process record cannot be deleted."""

    def __init__(self, message="BatchProcess record not deleted"):
        super().__init__(message)


class BatchSessionNotFoundException(Exception):
    """Raised when an Batch Session record is not found."""

    def __init__(self, message="BatchSession record not found"):
        super().__init__(message)


class BatchSessionSaveException(Exception):
    """Raised when an Batch Session record cannot be saved."""

    def __init__(self, message="BatchSession record not saved"):
        super().__init__(message)


class BatchSessionDeleteException(Exception):
    """Raised when an Batch Session record cannot be deleted."""

    def __init__(self, message="BatchSession record not deleted"):
        super().__init__(message)


class BatchProcessStorageBase(ABC):
    """Low-level service responsible for interfacing with the Batch Process record store."""

    @abstractmethod
    def delete(self, batch_id: str) -> None:
        """Deletes a Batch Process record."""
        pass

    @abstractmethod
    def save(
        self,
        batch_process: BatchProcess,
    ) -> BatchProcess:
        """Saves a Batch Process record."""
        pass

    @abstractmethod
    def get(
        self,
        batch_id: str,
    ) -> BatchProcess:
        """Gets a Batch Process record."""
        pass

    @abstractmethod
    def cancel(
        self,
        batch_id: str,
    ):
        """Cancel Batch Process record."""
        pass

    @abstractmethod
    def create_session(
        self,
        session: BatchSession,
    ) -> BatchSession:
        """Creates a Batch Session attached to a Batch Process."""
        pass

    @abstractmethod
    def get_session(self, session_id: str) -> BatchSession:
        """Gets session by session_id"""
        pass

    @abstractmethod
    def get_created_session(self, batch_id: str) -> BatchSession:
        """Gets all created Batch Sessions for a given Batch Process id."""
        pass

    @abstractmethod
    def get_created_sessions(self, batch_id: str) -> List[BatchSession]:
        """Gets all created Batch Sessions for a given Batch Process id."""
        pass

    @abstractmethod
    def update_session_state(
        self,
        batch_id: str,
        session_id: str,
        changes: BatchSessionChanges,
    ) -> BatchSession:
        """Updates the state of a Batch Session record."""
        pass


class SqliteBatchProcessStorage(BatchProcessStorageBase):
    _filename: str
    _conn: sqlite3.Connection
    _cursor: sqlite3.Cursor

    def __init__(self, filename: str) -> None:
        super().__init__()
        self._filename = filename
        self._conn = sqlite3.connect(filename, check_same_thread=False)
        # Enable row factory to get rows as dictionaries (must be done before making the cursor!)
        self._conn.row_factory = sqlite3.Row
        self._cursor = self._conn.cursor()

        with SqliteLock():
            # Enable foreign keys
            self._conn.execute("PRAGMA foreign_keys = ON;")
            self._create_tables()
            self._conn.commit()

    def _create_tables(self) -> None:
        """Creates the `batch_process` table and `batch_session` junction table."""

        # Create the `batch_process` table.
        self._cursor.execute(
            """--sql
            CREATE TABLE IF NOT EXISTS batch_process (
                batch_id TEXT NOT NULL PRIMARY KEY,
                batches TEXT NOT NULL,
                graph TEXT NOT NULL,
                canceled BOOLEAN NOT NULL DEFAULT(0),
                created_at DATETIME NOT NULL DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
                -- Updated via trigger
                updated_at DATETIME NOT NULL DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
                -- Soft delete, currently unused
                deleted_at DATETIME
            );
            """
        )

        self._cursor.execute(
            """--sql
            CREATE INDEX IF NOT EXISTS idx_batch_process_created_at ON batch_process (created_at);
            """
        )

        # Add trigger for `updated_at`.
        self._cursor.execute(
            """--sql
            CREATE TRIGGER IF NOT EXISTS tg_batch_process_updated_at
            AFTER UPDATE
            ON batch_process FOR EACH ROW
            BEGIN
                UPDATE batch_process SET updated_at = current_timestamp
                    WHERE batch_id = old.batch_id;
            END;
            """
        )

        # Create the `batch_session` junction table.
        self._cursor.execute(
            """--sql
            CREATE TABLE IF NOT EXISTS batch_session (
                batch_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
                -- updated via trigger
                updated_at DATETIME NOT NULL DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
                -- Soft delete, currently unused
                deleted_at DATETIME,
                -- enforce one-to-many relationship between batch_process and batch_session using PK
                -- (we can extend this to many-to-many later)
                PRIMARY KEY (batch_id,session_id),
                FOREIGN KEY (batch_id) REFERENCES batch_process (batch_id) ON DELETE CASCADE
            );
            """
        )

        # Add index for batch id
        self._cursor.execute(
            """--sql
            CREATE INDEX IF NOT EXISTS idx_batch_session_batch_id ON batch_session (batch_id);
            """
        )

        # Add index for batch id, sorted by created_at
        self._cursor.execute(
            """--sql
            CREATE INDEX IF NOT EXISTS idx_batch_session_batch_id_created_at ON batch_session (batch_id,created_at);
            """
        )

        # Add trigger for `updated_at`.
        self._cursor.execute(
            """--sql
            CREATE TRIGGER IF NOT EXISTS tg_batch_session_updated_at
            AFTER UPDATE
            ON batch_session FOR EACH ROW
            BEGIN
                UPDATE batch_session SET updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
                    WHERE batch_id = old.batch_id AND session_id = old.session_id;
            END;
            """
        )

    def delete(self, batch_id: str) -> None:
        with SqliteLock():
            try:
                self._cursor.execute(
                    """--sql
                    DELETE FROM batch_process
                    WHERE batch_id = ?;
                    """,
                    (batch_id,),
                )
                self._conn.commit()
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchProcessDeleteException from e
            except Exception as e:
                self._conn.rollback()
                raise BatchProcessDeleteException from e

    def save(
        self,
        batch_process: BatchProcess,
    ) -> BatchProcess:
        with SqliteLock():
            try:
                batches = [batch.json() for batch in batch_process.batches]
                self._cursor.execute(
                    """--sql
                    INSERT OR IGNORE INTO batch_process (batch_id, batches, graph)
                    VALUES (?, ?, ?);
                    """,
                    (batch_process.batch_id, json.dumps(batches), batch_process.graph.json()),
                )
                self._conn.commit()
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchProcessSaveException from e
        return self.get(batch_process.batch_id)

    def _deserialize_batch_process(self, session_dict: dict) -> BatchProcess:
        """Deserializes a batch session."""

        # Retrieve all the values, setting "reasonable" defaults if they are not present.

        batch_id = session_dict.get("batch_id", "unknown")
        batches_raw = session_dict.get("batches", "unknown")
        graph_raw = session_dict.get("graph", "unknown")
        canceled = session_dict.get("canceled", 0)
        batches = json.loads(batches_raw)
        batches = [parse_raw_as(Batch, batch) for batch in batches]
        return BatchProcess(
            batch_id=batch_id, batches=batches, graph=parse_raw_as(Graph, graph_raw), canceled=canceled == 1
        )

    def get(
        self,
        batch_id: str,
    ) -> BatchProcess:
        with SqliteLock():
            try:
                self._cursor.execute(
                    """--sql
                    SELECT *
                    FROM batch_process
                    WHERE batch_id = ?;
                    """,
                    (batch_id,),
                )

                result = cast(Union[sqlite3.Row, None], self._cursor.fetchone())
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchProcessNotFoundException from e
        if result is None:
            raise BatchProcessNotFoundException
        return self._deserialize_batch_process(dict(result))

    def cancel(
        self,
        batch_id: str,
    ):
        with SqliteLock():
            try:
                self._cursor.execute(
                    f"""--sql
                    UPDATE batch_process
                    SET canceled = 1
                    WHERE batch_id = ?;
                    """,
                    (batch_id,),
                )
                self._conn.commit()
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchSessionSaveException from e

    def create_session(
        self,
        session: BatchSession,
    ) -> BatchSession:
        with SqliteLock():
            try:
                self._cursor.execute(
                    """--sql
                    INSERT OR IGNORE INTO batch_session (batch_id, session_id, state)
                    VALUES (?, ?, ?);
                    """,
                    (session.batch_id, session.session_id, session.state),
                )
                self._conn.commit()
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchSessionSaveException from e
        return self.get_session(session.session_id)

    def get_session(self, session_id: str) -> BatchSession:
        with SqliteLock():
            try:
                self._cursor.execute(
                    """--sql
                    SELECT *
                    FROM batch_session
                    WHERE session_id= ?;
                    """,
                    (session_id,),
                )

                result = cast(Union[sqlite3.Row, None], self._cursor.fetchone())
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchSessionNotFoundException from e
        if result is None:
            raise BatchSessionNotFoundException
        return self._deserialize_batch_session(dict(result))

    def _deserialize_batch_session(self, session_dict: dict) -> BatchSession:
        """Deserializes a batch session."""

        # Retrieve all the values, setting "reasonable" defaults if they are not present.

        batch_id = session_dict.get("batch_id", "unknown")
        session_id = session_dict.get("session_id", "unknown")
        state = session_dict.get("state", "unknown")

        return BatchSession(
            batch_id=batch_id,
            session_id=session_id,
            state=state,
        )

    def get_created_session(self, batch_id: str) -> BatchSession:
        with SqliteLock():
            try:
                self._cursor.execute(
                    """--sql
                    SELECT *
                    FROM batch_session
                    WHERE batch_id = ? AND state = 'created';
                    """,
                    (batch_id,),
                )

                result = cast(Optional[sqlite3.Row], self._cursor.fetchone())
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchSessionNotFoundException from e
        if result is None:
            raise BatchSessionNotFoundException
        session = self._deserialize_batch_session(dict(result))
        return session

    def get_created_sessions(self, batch_id: str) -> List[BatchSession]:
        with SqliteLock():
            try:
                self._cursor.execute(
                    """--sql
                    SELECT *
                    FROM batch_session
                    WHERE batch_id = ? AND state = created;
                    """,
                    (batch_id,),
                )

                result = cast(list[sqlite3.Row], self._cursor.fetchall())
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchSessionNotFoundException from e
        if result is None:
            raise BatchSessionNotFoundException
        sessions = list(map(lambda r: self._deserialize_batch_session(dict(r)), result))
        return sessions

    def update_session_state(
        self,
        batch_id: str,
        session_id: str,
        changes: BatchSessionChanges,
    ) -> BatchSession:
        with SqliteLock():
            try:
                # Change the state of a batch session
                if changes.state is not None:
                    self._cursor.execute(
                        f"""--sql
                        UPDATE batch_session
                        SET state = ?
                        WHERE batch_id = ? AND session_id = ?;
                        """,
                        (changes.state, batch_id, session_id),
                    )

                    self._conn.commit()
            except sqlite3.Error as e:
                self._conn.rollback()
                raise BatchSessionSaveException from e
        return self.get_session(session_id)
