from typing import Optional

import backoff
from common import BACKOFF_CONFIG, RedisConnectionConfig
from redis import Redis
from state.state_interface import SateInterface


class RedisState(SateInterface):
    """Класс для сохранения состояния ETL с использованием Redis хранилища."""

    def __init__(
        self,
        connection_conf: RedisConnectionConfig,
        redis_connection: Optional[Redis] = None,
    ) -> None:
        self._config = connection_conf
        self._connection = redis_connection

    @property
    def connection(self) -> Redis:
        """Соединение с Redis."""
        if not self._connection or not self.is_connection_alive(self._connection):
            self._connection = self._connect()
        return self._connection

    @property
    def config(self) -> RedisConnectionConfig:
        """Конфиг для подключения к Redis."""
        return self._config

    @backoff.on_exception(**BACKOFF_CONFIG)
    def get_key(self, key: str, default: Optional[str] = None) -> Optional[str]:
        if value := self.connection.get(key):
            return value.decode(encoding="utf-8")
        return default

    @backoff.on_exception(**BACKOFF_CONFIG)
    def set_key(self, key: str, value: str) -> None:
        self._connection.set(key, value.encode(encoding="utf-8"))

    @staticmethod
    def is_connection_alive(redis_connection: Redis) -> bool:
        """
        Метод для проверки работающего соединения с Redis.
        :param redis_connection: объект соедиения с Redis.
        :return: True - соединение работает, False - соединение упало
        """
        try:
            redis_connection.ping()
        except Exception:
            return False
        return True

    @backoff.on_exception(**BACKOFF_CONFIG)
    def _connect(self) -> Redis:
        """Метод для создания соединения с Redis."""
        return Redis(**self._config.dict())
