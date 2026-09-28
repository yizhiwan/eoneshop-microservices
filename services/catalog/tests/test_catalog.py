import os

os.environ["DATABASE_URL"] = "sqlite:///./test_catalog.db"

import pytest
from fastapi.testclient import TestClient

from app.db import Base, engine
from app.main import app


@pytest.fixture
def client():
    Base.metadata.drop_all(engine)
    with TestClient(app) as c:
        yield c


def test_reserve_and_release(client):
    assert client.post("/products/1/reserve", json={"qty": 3}).json()["stock"] == 17
    assert client.post("/products/1/release", json={"qty": 3}).json()["stock"] == 20


def test_reserve_insufficient(client):
    assert client.post("/products/3/reserve", json={"qty": 6}).status_code == 409


def test_unknown_product(client):
    assert client.post("/products/99/reserve", json={"qty": 1}).status_code == 404
