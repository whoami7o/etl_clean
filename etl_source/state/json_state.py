import json
import os
from pathlib import Path
from typing import Optional, Union

from state.state_interface import SateInterface


class JSONSate(SateInterface):
    """Класс для сохранения состояния ETL с использованием JSON файла."""

    def __init__(
        self, file_path: Optional[Union[str, Path, os.PathLike]] = None
    ) -> None:

        if file_path:
            self._file_path = (
                Path(file_path) if not isinstance(file_path, Path) else file_path
            )
        else:
            self._file_path = Path(__file__).parent.joinpath("latest_state.json")

    @property
    def file_path(self) -> Path:
        """Путь к файлу где сохранено последнее состояние."""
        return self._file_path

    def get_key(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self.get_current_state().get(key, default)

    def set_key(self, key: str, value: str) -> None:
        current_state = self.get_current_state()
        current_state.update({key: value})

        with open(self._file_path, "w", encoding="utf-8") as state_file:
            json.dump(obj=current_state, fp=state_file)

    def get_current_state(self) -> dict:
        """Выгружает из файла словарь с текущим состоянием."""
        if not os.path.exists(self._file_path) or os.stat(self._file_path).st_size == 0:
            return {}

        with open(self._file_path, "r", encoding="utf-8") as state_file:
            current_state = json.load(state_file)
        return current_state
