from common.backoff_confing import BACKOFF_CONFIG
from common.connections import (
    ElasticConnectionConfig,
    PostgresConnectionConfig,
    RedisConnectionConfig,
)
from common.etl_config import ETL_CONFIG
from common.logger_config import LOGGER_CONFIG

POSTGRES_CONFIG = PostgresConnectionConfig()
REDIS_CONFIG = RedisConnectionConfig()
ES_CONFIG = ElasticConnectionConfig()
