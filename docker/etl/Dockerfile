###########
# BUILDER #
###########

FROM python:3.9.6-alpine as builder

WORKDIR /usr/src/app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DOCKERIZE_VERSION=v0.6.1 \
    BUILD_ONLY_PACKAGES='wget' \
    TINI_VERSION=v0.19.0

RUN apk update --no-cache && apk upgrade --no-cache\
  && apk add --no-cache \
    wget \
    bash \
    curl \
    gettext \
    git \
    # Defining build-time-only dependencies:
    $BUILD_ONLY_PACKAGES \
  # Installing `dockerize` utility:
  # https://github.com/jwilder/dockerize
  && wget "https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-alpine-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" \
  && tar -C /usr/local/bin -xzvf "dockerize-alpine-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" \
  && rm "dockerize-alpine-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" && dockerize --version \
  # Installing `tini` utility:
  # https://github.com/krallin/tini
  && apk add --no-cache tini \
  # Removing build-time-only dependencies:
  && apk del $BUILD_ONLY_PACKAGES \
  && apk add postgresql-dev gcc python3-dev musl-dev \
  && pip install --upgrade pip

COPY ../../etl_source/. .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /usr/src/app/wheels -r requirements.txt

#########
# FINAL #
#########

FROM builder as prod-build

RUN mkdir -p /home/app \
    && addgroup -S app && adduser -S app -G app

ENV HOME=/home/app \
    APP_HOME=/home/app/etl

RUN mkdir $APP_HOME
WORKDIR $APP_HOME

RUN apk update && apk add libpq
COPY --from=builder /usr/src/app/wheels /wheels
COPY --from=builder /usr/src/app/requirements.txt .
RUN pip install --no-cache /wheels/*

COPY ../../docker/etl/entrypoint.sh .
RUN sed -i 's/\r$//g'  $APP_HOME/entrypoint.sh \
    && chmod +x  $APP_HOME/entrypoint.sh

COPY ../../etl_source/. $APP_HOME

RUN chown -R app:app $APP_HOME

USER app

ENTRYPOINT ["/sbin/tini", "--", "/home/app/etl/entrypoint.sh"]
