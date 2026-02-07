from app.db import init_db
from app.logging_utils import configure_logging, get_logger, log_event

configure_logging()
logger = get_logger("api")

if __name__ == "__main__":
    init_db()
    log_event(logger, "db_initialized")
