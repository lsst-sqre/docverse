"""Alembic migration environment."""

from safir.database import run_migrations_offline, run_migrations_online

from alembic import context
from docverse.config import config

# Load all ORM models so that Base.metadata is populated.
from docverse.dbschema import Base

if context.is_offline_mode():
    run_migrations_offline(Base.metadata, config.database_url)
else:
    run_migrations_online(
        Base.metadata,
        config.database_url,
        config.database_password,
    )
