import os
import psycopg


def connect():
    return psycopg.connect(
        os.getenv(
            "DATABASE_URL",
            "postgresql://sport_user:SportData2026Local@localhost:55432/sport_data",
        )
    )
