from typing import Callable, Optional


def compose_movies_query(load_from: Optional[str]) -> str:
    """
    Формирует sql запрос с подставленной временной меткой для индекса movies
    """
    print(load_from)

    return f"""
    SELECT film_work.id,
        film_work.rating AS imdb_rating,
        film_work.title,
        film_work.description,
        film_work.type,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', genre.id, 'name', genre.name)) AS genres,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', person.full_name)) FILTER (WHERE person_film.role = 'director') AS directors,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', person.full_name)) FILTER (WHERE person_film.role = 'actor') AS actors,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', person.full_name)) FILTER (WHERE person_film.role = 'writer') AS writers,
        GREATEST(film_work.modified, MAX(person.modified), MAX(genre.modified)) AS updated_at
    FROM content.film_work film_work
        LEFT JOIN content.genre_film_work AS genre_film ON film_work.id = genre_film.film_work_id
        LEFT JOIN content.genre AS genre ON genre_film.genre_id = genre.id
        LEFT JOIN content.person_film_work AS person_film ON film_work.id = person_film.film_work_id
        LEFT JOIN content.person AS person ON person_film.person_id = person.id
    WHERE
        GREATEST(film_work.modified, person.modified, genre.modified) > '{load_from}'
    GROUP BY film_work.id
    ORDER BY GREATEST(film_work.modified, MAX(person.modified), MAX(genre.modified)) ASC
    """


def compose_genres_query(load_from: Optional[str]) -> str:
    """
    Формирует sql запрос с подставленной временной меткой для индекса genres
    """

    return f"""
    SELECT content.genre.id,
        content.genre.name,
        content.genre.modified as updated_at
    FROM content.genre
    WHERE
        content.genre.modified > '{load_from}'
    GROUP BY content.genre.id, content.genre.name, content.genre.modified
    ORDER BY content.genre.modified ASC
    """


def compose_persons_query(load_from: Optional[str]) -> str:
    """
    Формирует sql запрос с подставленной временной меткой для индекса persons
    """

    return f"""
    SELECT content.person.id,
        content.person.full_name AS name,
        ARRAY_AGG(DISTINCT person_film.role::text) AS role,
        ARRAY_AGG(DISTINCT person_film.film_work_id::text) AS film_ids,
        content.person.modified as updated_at
    FROM content.person
        LEFT JOIN content.person_film_work AS person_film ON person.id = person_film.person_id
    WHERE
        content.person.modified > '{load_from}'
    GROUP BY content.person.id, content.person.full_name, content.person.modified
    ORDER BY content.person.modified ASC
    """


def compose_query_for_index(
    index: str,
    composer_map: dict[str, Callable],
    load_from: Optional[str] = None,
) -> str:
    """Формирует нужный sql запрос в зависимости от индекса"""
    if (query_composer := composer_map.get(index)) and load_from:
        return query_composer(load_from)

    raise KeyError("Can't compose query for index: {0}".format(index.upper()))
