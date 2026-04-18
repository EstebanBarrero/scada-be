from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from app.config import settings


class Base(DeclarativeBase):
    pass


def _build_engine():
    kwargs = {}
    if "sqlite" in settings.DATABASE_URL:
        # SQLite requires check_same_thread=False for FastAPI's threading model
        kwargs["connect_args"] = {"check_same_thread": False}

    engine = create_engine(settings.DATABASE_URL, echo=False, **kwargs)

    if "sqlite" in settings.DATABASE_URL:
        # WAL mode: allows concurrent reads during writes — critical for dashboard queries
        # foreign_keys: SQLite disables FK enforcement by default; required for referential integrity
        # cache_size: 64MB page cache keeps hot pages in memory, reducing disk I/O
        @event.listens_for(engine, "connect")
        def set_sqlite_pragmas(dbapi_conn, _):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA cache_size=-64000")  # 64MB page cache
            cursor.close()

    return engine


engine = _build_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """FastAPI dependency: yields a DB session and guarantees cleanup."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables. Called on app startup."""
    # Import models so SQLAlchemy registers them before create_all()
    from app.models import alarm, tag  # noqa: F401

    Base.metadata.create_all(bind=engine)
