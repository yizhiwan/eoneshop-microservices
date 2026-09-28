from shared.db import make_database

Base, engine, SessionLocal, get_db = make_database("sqlite:///./payment.db")
