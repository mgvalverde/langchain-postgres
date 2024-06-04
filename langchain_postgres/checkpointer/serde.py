import json
from typing import Any
from psycopg.types.json import Jsonb
from langgraph.serde.jsonplus import JsonPlusSerializer


class PsycoPgSerializer(JsonPlusSerializer):
    """
    Serializer to handle psycopg scenarios
    """

    # Note:  this serializer is asymmetric and does not fully respec the interface
    # but due to requirements inserting/reading data from psql, this modification
    # is justified.

    def dumps(self, obj):
        # Logic
        #  1. dump the values using the parent class so we parse the classes before insertion into db
        #  2. rebuild the byte dictionary with the classes parsed
        #  3. build the Jsonb from the previous dict, so it is psql compatible.

        data_byte = super().dumps(obj)
        obj_deconstruct = json.loads(data_byte)
        return Jsonb(obj_deconstruct)

    def loads(self, data) -> Any:
        # Logic
        #  1. Retrieve a dictionary with deconstructed classes from psql
        #  2. Dump it into a byte json
        #  3. Rebuilt it with the parent class

        if isinstance(data, dict):
            data_plain = json.dumps(data).encode()
            data_ = super().loads(data_plain)
            return data_
        elif isinstance(data, Jsonb):
            return self.loads(data.obj)
        else:
            raise NotImplementedError("Currently only able to handle dicts")
