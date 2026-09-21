import os
import random
from datetime import datetime, timedelta, timezone

import psycopg

random.seed(42)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sport_user:SportData2026Local@localhost:55432/sport_data",
)

DISTANCE_SPORTS = {"Marche", "Running", "Randonnée", "Triathlon"}
SPORT_NORMALIZATION = {"Runing": "Running"}


def normalize_sport(sport: str) -> str:
    return SPORT_NORMALIZATION.get(sport, sport)


def generate_distance(sport: str):
    if sport == "Marche":
        return random.randint(1000, 15000)
    if sport == "Running":
        return random.randint(1000, 30000)
    if sport == "Randonnée":
        return random.randint(2000, 30000)
    if sport == "Triathlon":
        return random.randint(5000, 30000)
    return None


with psycopg.connect(DATABASE_URL) as connection:
    rows = connection.execute(
        """
        SELECT e.employee_id, COALESCE(NULLIF(s.sport, ''), 'Marche')
        FROM raw.employees e
        LEFT JOIN raw.employee_sports s USING(employee_id)
        ORDER BY e.employee_id
        """
    ).fetchall()

    inserted = 0
    now = datetime.now(timezone.utc)
    for employee_id, raw_sport in rows:
        sport = normalize_sport(raw_sport)
        for _ in range(random.randint(5, 55)):
            start = now - timedelta(
                days=random.randint(0, 364),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )
            duration_seconds = random.randint(1200, 10800)
            distance = generate_distance(sport)
            connection.execute(
                """
                INSERT INTO raw.activities(
                    employee_id, start_at, end_at, sport_type,
                    distance_m, comment, source
                ) VALUES (%s, %s, %s, %s, %s, %s, 'simulation_historical')
                """,
                (
                    employee_id,
                    start,
                    start + timedelta(seconds=duration_seconds),
                    sport,
                    distance,
                    "Historique simulé",
                ),
            )
            inserted += 1

print(f"{inserted} activités historiques simulées.")
