# langchain-postgres

[![Release Notes](https://img.shields.io/github/release/langchain-ai/langchain-postgres)](https://github.com/langchain-ai/langchain-postgres/releases)
[![CI](https://github.com/langchain-ai/langchain-postgres/actions/workflows/ci.yml/badge.svg)](https://github.com/langchain-ai/langchain-postgres/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Twitter](https://img.shields.io/twitter/url/https/twitter.com/langchainai.svg?style=social&label=Follow%20%40LangChainAI)](https://twitter.com/langchainai)
[![](https://dcbadge.vercel.app/api/server/6adMQxSpJS?compact=true&style=flat)](https://discord.gg/6adMQxSpJS)
[![Open Issues](https://img.shields.io/github/issues-raw/langchain-ai/langchain-postgres)](https://github.com/langchain-ai/langchain-postgres/issues)

The `langchain-postgres` package implementations of core LangChain abstractions using `Postgres`.

The package is released under the MIT license. 

Feel free to use the abstraction as provided or else modify them / extend them as appropriate for your own application.

## Requirements

The package currently only supports the [psycogp3](https://www.psycopg.org/psycopg3/) driver.

## Installation

```bash
pip install -U langchain-postgres
```

## Change Log

0.0.6: 
- Remove langgraph as a dependency as it was causing dependency conflicts.
- Base interface for checkpointer changed in langgraph, so existing implementation would've broken regardless.

## Usage

### ChatMessageHistory

The chat message history abstraction helps to persist chat message history 
in a postgres table.

PostgresChatMessageHistory is parameterized using a `table_name` and a `session_id`.

The `table_name` is the name of the table in the database where 
the chat messages will be stored.

The `session_id` is a unique identifier for the chat session. It can be assigned
by the caller using `uuid.uuid4()`.

```python
import uuid

from langchain_core.messages import SystemMessage, AIMessage, HumanMessage
from langchain_postgres import PostgresChatMessageHistory
import psycopg

# Establish a synchronous connection to the database
# (or use psycopg.AsyncConnection for async)
conn_info = ... # Fill in with your connection info
sync_connection = psycopg.connect(conn_info)

# Create the table schema (only needs to be done once)
table_name = "chat_history"
PostgresChatMessageHistory.create_tables(sync_connection, table_name)

session_id = str(uuid.uuid4())

# Initialize the chat history manager
chat_history = PostgresChatMessageHistory(
    table_name,
    session_id,
    sync_connection=sync_connection
)

# Add messages to the chat history
chat_history.add_messages([
    SystemMessage(content="Meow"),
    AIMessage(content="woof"),
    HumanMessage(content="bark"),
])

print(chat_history.messages)
```


### Vectorstore

See example for the [PGVector vectorstore here](https://github.com/langchain-ai/langchain-postgres/blob/main/examples/vectorstore.ipynb)

### Langgraph Checkpointer

#### Asynchronous

The `AsyncPostgresSaver` is an asynchronous implementation of the `Checkpointer` interface for Postgres using psycopg3.
This version does not support the synchronous methods.

```python
import asyncio
from psycopg_pool import AsyncConnectionPool
from langgraph.graph import StateGraph
from langchain_postgres import AsyncPostgresSaver

builder = StateGraph(int)
builder.add_node("add_one", lambda x: x + 1)
builder.set_entry_point("add_one")
builder.set_finish_point("add_one")

# Psql pool instantiation
conn_str = "postgresql://langchain:langchain@localhost/langchain"
pool = AsyncConnectionPool(conn_str, open=False)

async def open_pool():
    await pool.open()
    await pool.wait()
    print("Connection pool opened")


asyncio.run(open_pool())
memory = AsyncPostgresSaver(pool=pool)
asyncio.run(memory.setup())

# graph definition
builder = StateGraph(int)
builder.add_node("add_one", lambda x: x + 1)
builder.add_node("plus_3", lambda x: x *3)
builder.set_entry_point("add_one")
builder.add_edge("add_one", "plus_3")
builder.set_finish_point("plus_3")

graph = builder.compile(checkpointer=memory)

config = {"configurable": {"thread_id": "thread-2"}}
coro = graph.ainvoke(1, config)
result = asyncio.run(coro)

```

Examples using the checkpointer methods.
```python
# aput
config = {"configurable": {"thread_id": "4"}}
checkpoint = {"id": "1ef22622-3c49-644c-8001-9f11636714ba", "ts": "2023-05-03T10:00:00Z", "data": {"key": "value"}}
metadata = {"step": -1, "source": "input", "writes": 1}
asyncio.run(memory.aput(config=config, checkpoint=checkpoint, metadata=metadata))

# asearch
from langgraph.checkpoint.base import CheckpointMetadata
metadata_filter = CheckpointMetadata(step=1)

async def search():
    async for item in memory.asearch(metadata_filter=metadata_filter):
        print(item)
asyncio.run(search())


# alist

config = {"configurable": {"thread_id": "4"}}
async def list():
    async for item in memory.alist(config=config):
        print(item)
asyncio.run(list())

```