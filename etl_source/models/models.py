from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel


class FilmGenre(str, Enum):
    movie = "movie"
    tv_show = "tv_show"


class PersonRole(str, Enum):
    actor = "actor"
    director = "director"
    writer = "writer"


class AbstractModel(BaseModel):
    id: UUID


class PersonFilmWork(AbstractModel):
    name: str


class GenresES(AbstractModel):
    name: str


class PersonsES(PersonFilmWork):
    role: Optional[List[PersonRole]] = None
    film_ids: Optional[List[UUID]] = None


class MoviesES(AbstractModel):
    title: str
    imdb_rating: Optional[float] = None
    type: FilmGenre
    description: Optional[str] = None
    genres: Optional[List[GenresES]] = None
    directors: Optional[List[PersonFilmWork]] = None
    actors: Optional[List[PersonFilmWork]] = None
    writers: Optional[List[PersonFilmWork]] = None
    file_path: Optional[str] = None
