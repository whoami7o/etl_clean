from typing import Final, Iterator, Optional, Type

import backoff
import psycopg2
from common import BACKOFF_CONFIG, PostgresConnectionConfig
from models.models import AbstractModel
from pg_reader.mappings import MODEL_MAP
from psycopg2.extensions import connection as pg_conn
from psycopg2.extras import DictCursor


class PGReader:

    _model_map: Final = MODEL_MAP

    def __init__(
        self,
        connection_conf: PostgresConnectionConfig,
        postgres_connection: Optional[pg_conn] = None,
    ) -> None:
        self._config = connection_conf
        self._connection = postgres_connection

    @property
    def connection(self) -> pg_conn:
        if not self._connection or not self._connection.closed:
            self._connection = self._connect()
        return self._connection

    @property
    def config(self) -> PostgresConnectionConfig:
        return self._config

    @backoff.on_exception(**BACKOFF_CONFIG)
    def read_data(
        self,
        index: str,
        query: str,
        iter_size: Optional[int] = 1000,
    ) -> Iterator[tuple[dict, str]]:

        if model := self._model_map.get(index, None):
            return self._create_generator(model, query, iter_size)

        raise KeyError("No model for passed index: {0}".format(index.upper()))

    @backoff.on_exception(**BACKOFF_CONFIG)
    def _connect(self) -> pg_conn:
        if self._connection:
            self._connection.close()
        return psycopg2.connect(**self._config.dict(), cursor_factory=DictCursor)

    @backoff.on_exception(**BACKOFF_CONFIG)
    def _create_generator(
        self,
        model: Type[AbstractModel],
        query: str,
        iter_size: Optional[int] = 1000,
    ) -> Iterator[tuple[dict, str]]:
        cursor = self._connection.cursor()
        cursor.itersize = iter_size

        for row in cursor.execute(query):
            data = model(**row).dict()
            data["_id"] = data["id"]
            yield data, str(row["updated_at"])
