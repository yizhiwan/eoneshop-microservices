from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_small_amount_approved():
    r = client.post("/charges", json={"order_ref": "a", "amount_cents": 5000})
    assert r.json()["approved"]


def test_large_amount_declined():
    r = client.post("/charges", json={"order_ref": "b", "amount_cents": 10500})
    assert not r.json()["approved"]
