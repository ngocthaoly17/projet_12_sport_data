import os
import random
from datetime import datetime, timedelta, timezone

import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sport_user:SportData2026Local@localhost:55432/sport_data",
)
SPORT_NORMALIZATION = {"Runing": "Running"}


def distance_for(sport: str):
    if sport == "Marche": return random.randint(1000, 15000)
    if sport == "Running": return random.randint(1000, 15000)
    if sport == "Randonnée": return random.randint(2000, 20000)
    if sport == "Triathlon": return random.randint(5000, 30000)
    return None

with psycopg.connect(DATABASE_URL) as connection:
    employee_id, raw_sport = connection.execute(
        """
        SELECT e.employee_id, COALESCE(NULLIF(s.sport, ''), 'Marche')
        FROM raw.employees e
        LEFT JOIN raw.employee_sports s USING(employee_id)
        ORDER BY random()
        LIMIT 1
        """
    ).fetchone()

    sport = SPORT_NORMALIZATION.get(raw_sport, raw_sport)
    start = datetime.now(timezone.utc)
    duration = timedelta(minutes=random.randint(30, 100))
    distance = distance_for(sport)
    activity_id = connection.execute(
        """
        INSERT INTO raw.activities(
            employee_id, start_at, end_at, sport_type,
            distance_m, comment, source
        ) VALUES (%s, %s, %s, %s, %s, %s, 'live')
        RETURNING activity_id
        """,
        (employee_id, start, start + duration, sport, distance, "Activité live simulée"),
    ).fetchone()[0]

print(f"Activité live {activity_id} créée pour l'employé {employee_id} ({sport}).")
