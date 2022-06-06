# ETL-process for data migration from PostgreSQL to Elasticsearch

Отказоустойчивый перенос данных из Postgres в Elasticsearch

## Зависимости
```console
$ pip install -r etl-source/requirements.txt
```

## Docker

1. **elastic** - контейнер с Elasticsearch;
    - *.json - индексы для данных;
    - *.sh - entrypoint;
2. **etl** - python-контейнер с ETL;
    - *.sh - entrypoint;
3. **postgres_db** - ДБ;
    - data/* - database dump;
4. **redis** - контейнер с Redis.

Параметры подключения к БД находятся в .env

### Запуск
```console
$ make up
```

### Остановка приложения
```console
$ make down
```