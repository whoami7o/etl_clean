import logging

LOGGER_CONFIG = {
    "format": "%(asctime)s - %(name)s.%(funcName)s:%(lineno)d - %(levelname)s - %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
    "level": logging.INFO,
    "handlers": [logging.StreamHandler()],
}
