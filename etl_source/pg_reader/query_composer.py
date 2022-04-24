from typing import Callable, Final, Optional

from pg_reader.mappings import QUERY_MAP


def compose_movies_query(load_from: Optional[str]) -> str:
    """
    Формирует sql запрос с подставленной временной меткой для индекса movies
    """

    return f"""
    SELECT content.film_work.id,
        content.film_work.rating AS imdb_rating,
        content.film_work.title,
        content.film_work.description,
        content.film_work.type,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', content.genre.id, 'name', content.genre.name)) AS genres,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', content.person.full_name)) FILTER (WHERE content.person_film_work.role = 'director') AS directors,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', content.person.full_name)) FILTER (WHERE content.person_film_work.role = 'actor') AS actors,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', content.person.id, 'name', content.person.full_name)) FILTER (WHERE content.person_film_work.role = 'writer') AS writers,
        GREATEST(content.film_work.modified, MAX(content.person.modified), MAX(content.genre.modified)) AS updated_at
    FROM content.film_work
        LEFT JOIN content.genre_film_work AS genre_film ON content.film_work.id = genre_film.film_work_id
        LEFT JOIN content.genre AS genre ON genre_film.genre_id = genre.id
        LEFT JOIN content.person_film_work AS person_film ON content.film_work.id = person_film.film_work_id
        LEFT JOIN content.person AS person ON person_film.person_id = person.id
    WHERE
        GREATEST(content.film_work.modified, content.person.modified, content.genre.modified) > '{load_from}'
    GROUP BY content.film_work.id, content.film_work.rating, content.film_work.title, content.film_work.description, content.film_work.type, content.film_work.modified
    ORDER BY GREATEST(content.film_work.modified, MAX(content.film_work.modified), MAX(content.film_work.modified)) ASC
    """


def compose_genres_query(load_from: Optional[str]) -> str:
    """
    Формирует sql запрос с подставленной временной меткой для индекса genres
    """

    return f"""
    SELECT content.genre.id,
        content.genre.name,
        content.genre.modified
    FROM content.genre
    WHERE
        content.genre.modified > '{load_from}'
    GROUP BY genre.id, content.genre.name, content.genre.modified
    ORDER BY content.genre.modified ASC
    """


def compose_persons_query(load_from: Optional[str]) -> str:
    """
    Формирует sql запрос с подставленной временной меткой для индекса persons
    """

    return f"""
    SELECT content.person.id,
        content.person.full_name AS name,
        ARRAY_AGG(DISTINCT content.person_film_work.role::text) AS role,
        ARRAY_AGG(DISTINCT content.person_film_work.film_work_id::text) AS film_ids,
        content.person.modified
    FROM content.person
        LEFT JOIN content.person_film_work AS person_film ON person.id = person_film.person_id
    WHERE
        content.person.modified > '{load_from}'
    GROUP BY content.person.id, content.person.full_name, content.person.modified
    ORDER BY content.person.modified ASC
    """


def compose_query_for_index(
        index: str,
        load_from: Optional[str] = None,
        composer_map: Optional[dict[str, Callable]] = None,
) -> str:
    """Формирует нужный sql запрос в зависимости от индекса"""
    if not composer_map:
        composer_map = QUERY_MAP

    if query_composer := composer_map.get(index) and load_from:
        return query_composer(load_from)

    raise KeyError(
        "Can't compose query for index: {0}".format(index.upper())
    )
