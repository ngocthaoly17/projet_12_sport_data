from datetime import datetime

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

from .db import connect

app = FastAPI(
    title="Sport Data Solution - Event Driven POC",
    description=(
        "L'API simule la source Strava : elle écrit uniquement dans PostgreSQL. "
        "Debezium capture ensuite le changement et le publie dans Redpanda."
    ),
)

activities_created = Counter(
    "activities_created_total", "Nombre d'activités insérées dans PostgreSQL"
)


class Activity(BaseModel):
    employee_id: int
    start_at: datetime
    end_at: datetime
    sport_type: str = Field(min_length=1)
    distance_m: float | None = Field(default=None, ge=0)
    comment: str | None = None


@app.get("/health")
def health():
    with connect() as connection:
        connection.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/activities/recent")
def recent_activities(limit: int = 10):
    limit = max(1, min(limit, 100))
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT activity_id, employee_id, start_at, end_at, sport_type,
                   distance_m, comment, source, created_at
            FROM raw.activities
            ORDER BY activity_id DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()
    return [
        {
            "activity_id": r[0],
            "employee_id": r[1],
            "start_at": r[2],
            "end_at": r[3],
            "sport_type": r[4],
            "distance_m": float(r[5]) if r[5] is not None else None,
            "comment": r[6],
            "source": r[7],
            "created_at": r[8],
        }
        for r in rows
    ]


@app.post("/activities", status_code=201)
def create_activity(activity: Activity):
    if activity.end_at <= activity.start_at:
        raise HTTPException(status_code=422, detail="end_at must be after start_at")

    with connect() as connection:
        employee = connection.execute(
            "SELECT employee_id FROM raw.employees WHERE employee_id = %s",
            (activity.employee_id,),
        ).fetchone()
        if not employee:
            raise HTTPException(status_code=404, detail="employee_id inconnu")

        row = connection.execute(
            """
            INSERT INTO raw.activities(
                employee_id, start_at, end_at, sport_type,
                distance_m, comment, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'live')
            RETURNING activity_id
            """,
            (
                activity.employee_id,
                activity.start_at,
                activity.end_at,
                activity.sport_type,
                activity.distance_m,
                activity.comment,
            ),
        ).fetchone()

    activities_created.inc()
    return {
        "activity_id": row[0],
        "status": "inserted",
        "next": "Debezium -> Redpanda -> Slack + Spark",
    }
