from typing import List

from pydantic import BaseSettings, Field


class ETLConfig(BaseSettings):
    batch_size: int = Field(..., env="BATCH_SIZE")
    frequency: int = Field(..., env="FREQUENCY")
    backoff_max_retries: int = Field(..., env="BACKOFF_MAX_RETRIES")
    elastic_indexes: List[str] = Field(..., env="ELASTICSEARCH_INDEXES")


ETL_CONFIG = ETLConfig()
