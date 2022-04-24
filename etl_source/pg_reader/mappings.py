from typing import Callable, Final, Type

from models.models import AbstractModel, GenresES, MoviesES, PersonsES
from pg_reader.query_composer import (
    compose_genres_query,
    compose_movies_query,
    compose_persons_query,
)

MODEL_MAP: dict[str, Type[AbstractModel]] = {
    "movies": MoviesES,
    "genres": GenresES,
    "persons": PersonsES,
}

QUERY_MAP: dict[str, Callable] = {
    "movies": compose_movies_query,
    "genres": compose_genres_query,
    "persons": compose_persons_query,
}
