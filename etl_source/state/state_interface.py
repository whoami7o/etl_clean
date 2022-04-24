from abc import ABC, abstractmethod
from typing import Optional


class SateInterface(ABC):
    """ Базовый интерфейс состояния. """

    @abstractmethod
    def set_key(self, key: str, value: str) -> None:
        """ Создание пары {key -> value}. """
        ...

    @abstractmethod
    def get_key(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """ Получение значения по ключу. """
        ...
