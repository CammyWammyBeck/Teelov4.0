"""
Alembic migration environment configuration.

This file configures how Alembic runs migrations. It loads the database
URL from environment variables and sets up the SQLAlchemy metadata
for auto-generating migrations.

Key features:
- Loads DATABASE_URL from environment (no hardcoded credentials)
- Uses Teelo's SQLAlchemy models for autogenerate
- Supports both online (connected) and offline (SQL script) migrations
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# Import our models so Alembic can detect schema changes
# This import must happen before we access Base.metadata
from teelo.db.models import Base
from teelo.config import settings

# Alembic Config object - provides access to alembic.ini values
config = context.config

# Override sqlalchemy.url with our settings
# This allows us to use environment variables instead of hardcoding in alembic.ini
config.set_main_option("sqlalchemy.url", settings.database_url)

# Set up Python logging from alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# SQLAlchemy MetaData object for autogenerate support
# Alembic uses this to compare current database state vs model definitions
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    This generates SQL scripts without connecting to the database.
    Useful for reviewing migrations before applying them, or for
    environments where direct database access isn't available.

    Usage:
        alembic upgrade head --sql > migration.sql
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Compare types (e.g., VARCHAR(100) vs VARCHAR(200))
        compare_type=True,
        # Compare server defaults (DEFAULT values)
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    This connects to the database and applies migrations directly.
    This is the normal mode for development and production.

    Usage:
        alembic upgrade head
    """
    # Create engine from config
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,  # Don't pool connections for migrations
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # Compare column types for changes
            compare_type=True,
            # Compare server default values
            compare_server_default=True,
            # Include schemas in autogenerate
            include_schemas=True,
        )

        with context.begin_transaction():
            context.run_migrations()


# Determine which mode to run in based on Alembic's context
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
