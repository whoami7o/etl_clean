import logging
import time
from datetime import datetime
from typing import Optional

from common import (ES_CONFIG, ETL_CONFIG, LOGGER_CONFIG, POSTGRES_CONFIG,
                    REDIS_CONFIG)
from es_saver.saver import ElasticSaver
from pg_reader.mappings import QUERY_MAP
from pg_reader.query_composer import compose_query_for_index
from pg_reader.reader import PGReader
from state.redis_state import RedisState


def etl_process(
    query: str,
    index: str,
    postgres_reader: PGReader,
    es_saver: ElasticSaver,
    iter_size: Optional[int] = 1000,
) -> None:

    data_generator = postgres_reader.read_data(
        index=index, query=query, iter_size=iter_size
    )
    es_saver.upload_data(
        data=data_generator,
        index=index,
        iter_size=iter_size,
    )


def main() -> None:

    state = RedisState(connection_conf=REDIS_CONFIG)
    postgres_reader = PGReader(connection_conf=POSTGRES_CONFIG)
    elastic_saver = ElasticSaver(connection_conf=ES_CONFIG, state_io=state)

    iter_size = ETL_CONFIG.batch_size
    freq = ETL_CONFIG.frequency
    indexes = ETL_CONFIG.elastic_indexes

    logging.basicConfig(**LOGGER_CONFIG)
    logger = logging.getLogger(__name__)

    while True:
        logger.info("STARTING ETL SYNCHRONIZATION...")

        for index in indexes:
            load_from = state.get_key(
                "load_from_{0}".format(index), default=str(datetime.min)
            )

            try:
                query = compose_query_for_index(
                    index,
                    QUERY_MAP,
                    load_from,
                )
                etl_process(
                    query=query,
                    index=index,
                    postgres_reader=postgres_reader,
                    es_saver=elastic_saver,
                    iter_size=iter_size,
                )

            except ValueError as e:
                logger.error(
                    "ERROR while transferring index: {0}\nERROR INFO:\n{1}".format(
                        index, e
                    )
                )
                continue

        logger.info("LET'S CHILL FOR SOME TIME ({0} seconds)".format(freq))
        time.sleep(freq)


if __name__ == "__main__":
    main()
