import csv
import os
from pathlib import Path
from typing import Any

import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sport_user:SportData2026Local@localhost:55432/sport_data",
)
ROOT = Path(__file__).resolve().parents[1]


def to_int(value: Any) -> int | None:
    text = "" if value is None else str(value).strip()
    return int(float(text.replace(",", "."))) if text else None


def to_float(value: Any) -> float | None:
    text = "" if value is None else str(value).strip()
    return float(text.replace(",", ".")) if text else None


def empty_to_none(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


with psycopg.connect(DATABASE_URL) as connection:
    with connection.cursor() as cursor:
        with (ROOT / "data/input/employees.csv").open(
            encoding="utf-8-sig", newline=""
        ) as file:
            for row in csv.DictReader(file):
                cleaned = {
                    "employee_id": to_int(row.get("employee_id")),
                    "last_name": empty_to_none(row.get("last_name")),
                    "first_name": empty_to_none(row.get("first_name")),
                    "birth_date": empty_to_none(row.get("birth_date")),
                    "business_unit": empty_to_none(row.get("business_unit")),
                    "hire_date": empty_to_none(row.get("hire_date")),
                    "annual_gross_salary": to_float(row.get("annual_gross_salary")),
                    "contract_type": empty_to_none(row.get("contract_type")),
                    "paid_leave_days": to_int(row.get("paid_leave_days")),
                    "home_address": empty_to_none(row.get("home_address")),
                    "commute_mode": empty_to_none(row.get("commute_mode")),
                }
                cursor.execute(
                    """
                    INSERT INTO raw.employees (
                        employee_id, last_name, first_name, birth_date,
                        business_unit, hire_date, annual_gross_salary,
                        contract_type, paid_leave_days, home_address, commute_mode
                    ) VALUES (
                        %(employee_id)s, %(last_name)s, %(first_name)s, %(birth_date)s,
                        %(business_unit)s, %(hire_date)s, %(annual_gross_salary)s,
                        %(contract_type)s, %(paid_leave_days)s, %(home_address)s,
                        %(commute_mode)s
                    )
                    ON CONFLICT (employee_id) DO UPDATE SET
                        last_name = EXCLUDED.last_name,
                        first_name = EXCLUDED.first_name,
                        birth_date = EXCLUDED.birth_date,
                        business_unit = EXCLUDED.business_unit,
                        hire_date = EXCLUDED.hire_date,
                        annual_gross_salary = EXCLUDED.annual_gross_salary,
                        contract_type = EXCLUDED.contract_type,
                        paid_leave_days = EXCLUDED.paid_leave_days,
                        home_address = EXCLUDED.home_address,
                        commute_mode = EXCLUDED.commute_mode
                    """,
                    cleaned,
                )

        with (ROOT / "data/input/employee_sports.csv").open(
            encoding="utf-8-sig", newline=""
        ) as file:
            for row in csv.DictReader(file):
                cursor.execute(
                    """
                    INSERT INTO raw.employee_sports(employee_id, sport)
                    VALUES (%s, %s)
                    ON CONFLICT (employee_id) DO UPDATE SET sport = EXCLUDED.sport
                    """,
                    (to_int(row.get("employee_id")), empty_to_none(row.get("sport"))),
                )

print("Référentiels chargés dans PostgreSQL.")
