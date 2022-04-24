FROM elasticsearch:7.10.1 AS builder

ARG DISCOVERY_TYPE
ENV discovery.type=${DISCOVERY_TYPE}

ADD index_load.sh genres.json persons.json movies.json /

RUN chmod +x '/index_load.sh' \
    && /usr/local/bin/docker-entrypoint.sh elasticsearch -d -E path.data=/tmp/data \
    && while [[ "$(curl -s -o /dev/null -w '%{http_code}' localhost:9200)" != "200" ]]; do sleep 1; done \
    && /index_load.sh

FROM builder
COPY --from=builder /tmp/data/ /usr/share/elasticsearch/data/
