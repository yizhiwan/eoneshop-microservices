import os
import sys
from pathlib import Path

# Make the repo-level `shared` package importable, like the Docker image does.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
# CI also runs this suite against Postgres by setting DATABASE_URL.
os.environ.setdefault("DATABASE_URL", "sqlite:///./test_catalog.db")
os.environ["OUTBOX_RELAY"] = "off"
