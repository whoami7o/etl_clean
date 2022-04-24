import logging

import backoff
from common.etl_config import ETL_CONFIG

logger = logging.getLogger(__name__)

BACKOFF_CONFIG = {
    "wait_gen": backoff.expo,
    "exception": Exception,
    "logger": logger,
    "max_tries": ETL_CONFIG.backoff_max_retries,
}
