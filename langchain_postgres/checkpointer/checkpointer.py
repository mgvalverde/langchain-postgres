import asyncio
import logging
from contextlib import asynccontextmanager
import psycopg

from langgraph.checkpoint import Checkpoint

from psycopg_pool import AsyncConnectionPool
from langchain_core.runnables import RunnableConfig
from typing import Iterator, Optional, TypeVar, AsyncIterator, AsyncGenerator, Union, Tuple, Any
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    CheckpointTuple,
    SerializerProtocol,
    CheckpointMetadata,
)

from .serde import PsycoPgSerializer
from .utils import search_where, not_implemented_sync_method



@asynccontextmanager
async def _get_async_connection(
        connection: Union[psycopg.AsyncConnection, AsyncConnectionPool, None],
) -> AsyncGenerator[psycopg.AsyncConnection, None]:
    """Get the connection to the Postgres database."""
    if isinstance(connection, psycopg.AsyncConnection):
        yield connection
    elif isinstance(connection, AsyncConnectionPool):
        async with connection.connection() as conn:
            yield conn
    else:
        raise ValueError(
            "Invalid async connection object. Please initialize the check pointer "
            f"with an appropriate async connection object. "
            f"Got {type(connection)}."
        )


class AsyncPostgresSaver(BaseCheckpointSaver):
    """An asynchronous checkpoint saver that stores checkpoints in a PostgreSQL database.

    Tip:
        Requires the [psycopg](https://pypi.org/project/psycopg/) package.
        Install it with `pip install psycopg`.

    Args:
        conn_str (str): The PostgreSQL connection string.
        serde (Optional[SerializerProtocol]): The serializer to use for serializing and deserializing checkpoints. Defaults to JsonPlusSerializerCompat.

    Examples:
        Usage within a StateGraph:
        ```pycon
        >>> import asyncio
        >>> import psycopg
        >>>
        >>> from langgraph.graph import StateGraph
        >>>
        >>> builder = StateGraph(int)
        >>> builder.add_node("add_one", lambda x: x + 1)
        >>> builder.set_entry_point("add_one")
        >>> builder.set_finish_point("add_one")
        >>> conn_str = "postgres://user:password@host:port/dbname"
        >>> memory = AsyncPostgresSaver.from_conn_str(conn_str)
        >>> graph = builder.compile(checkpointer=memory)
        >>> coro = graph.ainvoke(1, {"configurable": {"thread_id": "thread-1"}})
        >>> asyncio.run(coro)
        Output: 2
        ```
    """

    serde = PsycoPgSerializer()

    pool: AsyncConnectionPool
    lock: asyncio.Lock
    is_setup: bool
    _schema: str = None
    _table: str = "checkpoint"

    @property
    def table_name(self):
        if self._schema:
            return f"{self._schema}.{self._table}"
        return self._table

    def __init__(
            self,
            pool: AsyncConnectionPool,
            *,
            serde: Optional[SerializerProtocol] = None,
    ):
        super().__init__(serde=serde)
        self.pool = pool
        self.conn = None
        self.lock = asyncio.Lock()
        self.is_setup = False

    @asynccontextmanager
    async def _get_async_connection(self) -> AsyncGenerator[psycopg.AsyncConnection, None]:
        """Get the connection to the Postgres database."""
        async with _get_async_connection(self.pool) as connection:
            yield connection

    @asynccontextmanager
    async def _get_async_cursor(self) -> AsyncGenerator[psycopg.AsyncCursor, None]:
        """Get the connection to the Postgres database."""
        async with _get_async_connection(self.pool) as connection:
            async with connection.cursor() as cursor:
                yield cursor

    async def setup(self) -> None:
        query = f"""
                CREATE TABLE if NOT EXISTS {self.table_name} (
                thread_id TEXT NOT NULL,
                thread_ts TEXT NOT NULL,
                parent_ts TEXT,
                checkpoint JSONB NOT NULL,
                metadata JSONB,
                PRIMARY KEY (thread_id, thread_ts)
            );
        """
        async with self.lock:
            if self.is_setup:
                return

            async with self._get_async_cursor() as cursor:
                await cursor.execute(query)

            self.is_setup = True
            logging.info("Checkpoint table created")

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:

        thread_id = str(config["configurable"]["thread_id"])
        thread_ts = config["configurable"].get("thread_ts")

        if thread_ts:
            thread_ts = str(thread_ts)
            query = f"""
                SELECT checkpoint, parent_ts, metadata FROM {self.table_name} 
                WHERE thread_id = %s AND thread_ts = %s
            """
            args = (thread_id, thread_ts)

            async with self._get_async_cursor() as cursor:
                await cursor.execute(query, args)
                value = await cursor.fetchone()

            if value:
                return CheckpointTuple(
                    config,
                    self.serde.loads(value[0]),
                    self.serde.loads(value[2]) if value[2] is not None else {},
                    (
                        {
                            "configurable": {
                                "thread_id": config["configurable"]["thread_id"],
                                "thread_ts": value[1],
                            }
                        }
                        if value[1]
                        else None
                    ),
                )
        else:
            query = f"""
                SELECT thread_id, thread_ts, parent_ts, checkpoint, metadata 
                FROM {self.table_name}
                WHERE thread_id = %s
                ORDER BY thread_ts 
                DESC LIMIT 1
            """
            args = (thread_id,)
            async with self._get_async_cursor() as cursor:
                await cursor.execute(query, args)
                value = await cursor.fetchone()
            if value:
                return CheckpointTuple(
                    {"configurable": {"thread_id": value[0],"thread_ts": value[1]}},
                    self.serde.loads(value[3]),
                    self.serde.loads(value[4]) if value[4] is not None else {},
                    (
                        {"configurable": {"thread_id": value[0], "thread_ts": value[2],}}
                        if value[2]
                        else None
                    ),
                )

    async def aput(self, config: RunnableConfig,
                   checkpoint: Checkpoint,
                   metadata: CheckpointMetadata, ) -> RunnableConfig:

        query = f"""
        INSERT INTO 
        {self.table_name} (thread_id, thread_ts, parent_ts, checkpoint, metadata) 
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (thread_id, thread_ts)  
        DO UPDATE SET
         parent_ts = EXCLUDED.parent_ts,
         checkpoint = EXCLUDED.checkpoint,
         metadata = EXCLUDED.metadata
        ;
        """
        args = (
            str(config["configurable"]["thread_id"]),
            checkpoint["id"],
            config["configurable"].get("thread_ts"),
            self.serde.dumps(checkpoint),
            self.serde.dumps(metadata),
        )
        async with self._get_async_cursor() as cursor:
            await cursor.execute(query, args)

        return {
            "configurable": {
                "thread_id": config["configurable"]["thread_id"],
                "thread_ts": checkpoint["id"],
            }
        }

    async def alist(
            self,
            config: RunnableConfig,
            *,
            before: Optional[RunnableConfig] = None,
            limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        if before is None:
            query = ("SELECT thread_id, thread_ts, parent_ts, checkpoint, metadata "
                     f"FROM {self.table_name} "
                     "WHERE thread_id = %s ORDER BY thread_ts DESC")
            args = (str(config["configurable"]["thread_id"]),)
        else:
            query = ("SELECT thread_id, thread_ts, parent_ts, checkpoint, metadata"
                     f" FROM {self.table_name}"
                     " WHERE thread_id = %s AND thread_ts < %s "
                     "ORDER BY thread_ts DESC")
            args = (
                str(config["configurable"]["thread_id"]),
                str(before["configurable"]["thread_ts"]),
            )
        if limit:
            query += f" LIMIT {limit}"


        async with self._get_async_cursor() as cursor:
            await cursor.execute(query, args)
            async for thread_id, thread_ts, parent_ts, value, metadata in cursor:
                yield CheckpointTuple(
                    config={"configurable": {"thread_id": thread_id, "thread_ts": thread_ts}},
                    checkpoint=self.serde.loads(value),
                    metadata=self.serde.loads(metadata) if metadata is not None else {},
                    parent_config=(
                        {
                            "configurable": {
                                "thread_id": thread_id,
                                "thread_ts": parent_ts,
                            }
                        }
                        if parent_ts
                        else None
                    ),
                )

    async def asearch(
            self,
            metadata_filter: CheckpointMetadata,
            *,
            before: Optional[RunnableConfig] = None,
            limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:

        SELECT = f"SELECT thread_id, thread_ts, parent_ts, checkpoint, metadata FROM {self.table_name} "
        WHERE, params = search_where(metadata_filter, before)
        ORDER_BY = "ORDER BY thread_ts DESC "
        LIMIT = f"LIMIT {limit}" if limit else ""

        query = f"{SELECT}{WHERE}{ORDER_BY}{LIMIT}"

        async with self._get_async_cursor() as cursor:
            await cursor.execute(query, params)
            async for thread_id, thread_ts, parent_ts, value, metadata in cursor:
                yield CheckpointTuple(
                    {"configurable": {"thread_id": thread_id, "thread_ts": thread_ts}},
                    self.serde.loads(value),
                    self.serde.loads(metadata) if metadata is not None else {},
                    (
                        {
                            "configurable": {
                                "thread_id": thread_id,
                                "thread_ts": parent_ts,
                            }
                        }
                        if parent_ts
                        else None
                    ),
                )

    @not_implemented_sync_method
    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """[NOT IMPLEMENTED] Get a checkpoint tuple from the database."""

    @not_implemented_sync_method
    def list(
            self,
            config: RunnableConfig,
            *,
            before: Optional[RunnableConfig] = None,
            limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """[NOT IMPLEMENTED] List checkpoints from the database."""

    @not_implemented_sync_method
    def search(
            self,
            metadata_filter: CheckpointMetadata,
            *,
            before: Optional[RunnableConfig] = None,
            limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        """ [NOT IMPLEMENTED] Search for checkpoints by metadata. """

    @not_implemented_sync_method
    def put(
            self,
            config: RunnableConfig,
            checkpoint: Checkpoint,
            metadata: CheckpointMetadata,
    ) -> RunnableConfig:
        """[NOT IMPLEMENTED] Save a checkpoint to the database."""
