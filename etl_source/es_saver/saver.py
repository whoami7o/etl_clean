import logging
import time
from typing import Iterator, Optional

import backoff
from common import BACKOFF_CONFIG, ElasticConnectionConfig
from elasticsearch import Elasticsearch, helpers
from state.state_interface import SateInterface

logger = logging.getLogger(__name__)


class ElasticSaver:

    def __init__(
        self,
        connection_conf: ElasticConnectionConfig,
        state_io: SateInterface,
        elastic_connection: Optional[Elasticsearch] = None,
    ) -> None:

        self._config = connection_conf
        self._state = state_io
        self._connection = elastic_connection

    @property
    def connection(self) -> Elasticsearch:
        """ Соединение с Elasticsearch. """
        if not self._connection or not self._connection.ping():
            self._connection = self._connect()
        return self._connection

    @property
    def config(self) -> ElasticConnectionConfig:
        """ Конфиг для подключения к Elasticsearch. """
        return self._config

    @backoff.on_exception(**BACKOFF_CONFIG)
    def upload_data(
            self,
            data: Iterator[tuple[dict, str]],
            index: str,
            iter_size: Optional[int] = 1000,
    ) -> None:
        item_generator = self._generate_data(data, index, iter_size)

        t_start = time.perf_counter()
        rows, *_ = helpers.bulk(
            client=self._connection,
            actions=item_generator,
            index=index,
            chunk_size=iter_size,
        )
        time_taken = time.perf_counter() - t_start

        if rows == 0:
            logger.info(
                "No data to update for {0}".format(index.upper())
            )
        else:
            logger.info(
                "{0} rows uploaded to Elasticsearch for {1} within {2} microseconds".format(
                    rows, index.upper(), round(time_taken, 4)
                )
            )

    @backoff.on_exception(**BACKOFF_CONFIG)
    def _generate_data(
            self,
            data: Iterator[tuple[dict, str]],
            index: str,
            iter_size: Optional[int] = 1000,
    ) -> Iterator[dict]:
        last_loaded: Optional[str] = None
        key: str = "load_from_{0}".format(index)

        for i, data_ in enumerate(data):
            item, last_record, *_ = data_
            last_loaded = last_record
            yield item

            if i % iter_size:
                self._state.set_key(key, last_loaded)

        if last_loaded:
            self._state.set_key(key, last_loaded)

    @backoff.on_exception(**BACKOFF_CONFIG)
    def _connect(self) -> Elasticsearch:
        """ Метод для создания соединения с Elasticsearch. """
        return Elasticsearch(
            [
                "{0}:{1}".format(
                    self._config.host,
                    self._config.port
                ),
            ]
        )




