import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

from app.logging_utils import get_logger, log_event

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

logger = get_logger("db")


def _alembic_config() -> Config:
    root = Path(__file__).resolve().parent.parent
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", DATABASE_URL)
    return config


def check_connection(log: bool = True) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        if log:
            log_event(logger, "db_connection_ok")
        return True
    except Exception as exc:
        if log:
            log_event(logger, "db_connection_failed", error=str(exc))
        return False


def run_migrations() -> None:
    config = _alembic_config()
    script = ScriptDirectory.from_config(config)
    head_rev = script.get_current_head()

    with engine.connect() as conn:
        context = MigrationContext.configure(conn)
        current_rev = context.get_current_revision()

    command.upgrade(config, "head")

    with engine.connect() as conn:
        context = MigrationContext.configure(conn)
        new_rev = context.get_current_revision()

    log_event(
        logger,
        "db_migrations_applied",
        from_revision=current_rev,
        to_revision=new_rev,
        head_revision=head_rev,
    )


def init_db() -> None:
    if not check_connection(log=True):
        raise RuntimeError("database connection failed")
    run_migrations()


def check_db() -> bool:
    return check_connection(log=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
