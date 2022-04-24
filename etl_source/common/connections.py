from pydantic import BaseSettings, Field


class PostgresConnectionConfig(BaseSettings):
    dbname: str = Field(..., env="POSTGRES_DB")
    user: str = Field(..., env="POSTGRES_USER")
    password: str = Field(..., env="POSTGRES_PASSWORD")
    host: str = Field(..., env="POSTGRES_HOST")
    port: int = Field(..., env="POSTGRES_PORT")
    # options: str = Field(..., env="POSTGRES_OPTIONS")


class ElasticConnectionConfig(BaseSettings):
    host: str = Field(..., env="ELASTICSEARCH_HOST")
    port: int = Field(..., env="ELASTICSEARCH_PORT")


class RedisConnectionConfig(BaseSettings):
    host: str = Field(..., env="REDIS_HOST")
    port: int = Field(..., env="REDIS_PORT")
