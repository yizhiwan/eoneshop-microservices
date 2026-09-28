"""Database setup shared by every service. Each service still gets its own
database: this only removes four copies of the same connection code.

SQLite for quick local dev and the default test run; Postgres (Neon in
production, a service container in CI) whenever DATABASE_URL says so.
"""
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


def make_database(default_url: str):
    url = os.getenv("DATABASE_URL", default_url)
    if url.startswith("postgres://") or url.startswith("postgresql://"):
        # Neon hands out plain postgresql:// URLs; use the psycopg 3 driver.
        url = "postgresql+psycopg://" + url.split("://", 1)[1]

    if url.startswith("sqlite"):
        engine = create_engine(url, connect_args={"check_same_thread": False})
    else:
        # Neon suspends idle compute, so stale connections are normal:
        # test each one before use. Keep the pool small (free tier limits).
        engine = create_engine(url, pool_pre_ping=True, pool_size=3, max_overflow=2)

    SessionLocal = sessionmaker(bind=engine, autoflush=False)

    class Base(DeclarativeBase):
        pass

    def get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    return Base, engine, SessionLocal, get_db
