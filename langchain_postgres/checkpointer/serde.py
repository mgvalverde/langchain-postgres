from typing import Any
from psycopg.types.json import Jsonb
from langgraph.serde.jsonplus import JsonPlusSerializer


class PsycoPgSerializer(JsonPlusSerializer):

    def loads(self, data) -> Any:
        # TODO: fix unhandled case.

        if isinstance(data, dict):
            return data
        else:
            raise NotImplementedError("currently only able to handle dicts")

    def dumps(self, obj):
        return Jsonb(obj)
