import functools
import json
from langchain_core.runnables import RunnableConfig
from typing import Optional, TypeVar, Tuple, Any
from langgraph.checkpoint.base import CheckpointMetadata

T = TypeVar("T", bound=callable)


def not_implemented_sync_method(func: T) -> T:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        raise NotImplementedError(
            "The AsyncPostgresSaver does not support synchronous methods."
        )

    return wrapper


def _metadata_predicate(
        metadata_filter: CheckpointMetadata,
) -> Tuple[str, Tuple[Any, ...]]:
    """Return WHERE clause predicates for (a)search() given metadata filter.

    This method returns a tuple of a string and a tuple of values. The string
    is the parametered WHERE clause predicate (excluding the WHERE keyword):
    "column1 = %s AND column2 IS %s". The tuple of values contains the values
    for each of the corresponding parameters.
    """

    def _where_value(query_value: Any) -> Tuple[str, Any, str]:
        """Return tuple of operator, value and type for WHERE clause predicate."""
        if query_value is None:
            return ("IS %s", None, "")
        elif isinstance(query_value, bool):
            return ("= %s", query_value, "BOOLEAN")
        elif isinstance(query_value, str):
            return ("= %s", query_value, "TEXT")
        elif isinstance(query_value, int):
            return ("= %s", query_value, "INTEGER")
        elif isinstance(query_value, float):
            return ("= %s", query_value, "NUMERIC")
        elif isinstance(query_value, dict) or isinstance(query_value, list):
            return ("= %s", json.dumps(query_value, separators=(",", ":")), "")
        else:
            return ("= %s", str(query_value), "")

    predicate = ""
    param_values = ()

    # process metadata query
    for query_key, query_value in metadata_filter.items():
        operator, param_value, value_type = _where_value(query_value)
        value_type = "::" + value_type if value_type else ""
        predicate += f"(metadata ->> '{query_key}'){value_type} {operator} AND "
        param_values += (param_value,)

    if predicate != "":
        # remove trailing AND
        predicate = predicate[:-4]

    # predicate contains an extra trailing space
    return (predicate, param_values)


def search_where(
        metadata_filter: CheckpointMetadata,
        before: Optional[RunnableConfig] = None,
) -> Tuple[str, Tuple[Any, ...]]:
    """Return WHERE clause predicates for (a)search() given metadata filter
    and `before` config.

    This method returns a tuple of a string and a tuple of values. The string
    is the parametered WHERE clause predicate (including the WHERE keyword):
    "WHERE column1 = %s AND column2 IS %s". The tuple of values contains the
    values for each of the corresponding parameters.
    """
    where = "WHERE "
    param_values = ()

    # construct predicate for metadata filter
    metadata_predicate, metadata_values = _metadata_predicate(metadata_filter)
    if metadata_predicate != "":
        where += metadata_predicate
        param_values += metadata_values

    # construct predicate for `before`
    if before is not None:
        if metadata_predicate != "":
            where += "AND thread_ts < %s "
        else:
            where += "thread_ts < %s "

        param_values += (before["configurable"]["thread_ts"],)

    if where == "WHERE ":
        # no predicates, return an empty WHERE clause string
        return ("", ())
    else:
        return (where, param_values)
