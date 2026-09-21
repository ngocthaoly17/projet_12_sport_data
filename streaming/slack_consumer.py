import json
import os
import time
from datetime import datetime

import psycopg
import requests
from confluent_kafka import Consumer, KafkaException


# ============================================================
# CONFIGURATION
# ============================================================

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "redpanda:9092",
)

TOPIC = os.getenv(
    "ACTIVITIES_TOPIC",
    "sport_data.raw.activities",
)

WEBHOOK = os.getenv(
    "SLACK_WEBHOOK_URL",
    "",
).strip()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sport_user:SportData2026Local@db:5432/sport_data",
)


# ============================================================
# RÉCUPÉRATION DU NOM DU COLLABORATEUR
# ============================================================

def employee_name(employee_id):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            row = conn.execute(
                """
                SELECT first_name, last_name
                FROM raw.employees
                WHERE employee_id = %s
                """,
                (employee_id,),
            ).fetchone()

            if row:
                return f"{row[0]} {row[1]}"

            return f"Employé {employee_id}"

    except Exception as exc:
        print(
            f"Erreur récupération employé {employee_id}: {exc}",
            flush=True,
        )
        return f"Employé {employee_id}"


# ============================================================
# CONVERSION DES TIMESTAMPS
# ============================================================

def parse_ts(value):
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except ValueError:
            return None

    return None


# ============================================================
# CONSTRUCTION DU MESSAGE SLACK
# ============================================================

def build_message(activity):
    name = employee_name(
        activity.get("employee_id")
    )

    sport = (
        activity.get("sport_type")
        or "sport"
    )

    start = parse_ts(
        activity.get("start_at")
    )

    end = parse_ts(
        activity.get("end_at")
    )

    # Durée de l'activité en minutes
    duration = None

    if start and end:
        duration = max(
            1,
            round(
                (end - start).total_seconds() / 60
            ),
        )

    distance = activity.get("distance_m")

    # Sports pour lesquels une distance en km est pertinente
    distance_sports = {
        "Marche",
        "Running",
        "Randonnée",
        "Triathlon",
    }

    # Message de base
    text = (
        f"🏃 Bravo {name} ! "
        f"Tu viens de terminer une activité {sport}"
    )

    # Distance uniquement pour les sports kilométriques
    if (
        distance is not None
        and sport in distance_sports
    ):
        distance_km = float(distance) / 1000

        text += (
            f" de {distance_km:.1f} km"
        )

    # Durée
    if duration is not None:
        text += (
            f" en {duration} min"
        )

    text += " ! 🔥🏅"

    # Commentaire éventuel
    comment = (
        activity.get("comment")
        or ""
    ).strip()

    ignored_comments = {
        "Activité live simulée",
        "Historique simulé",
    }

    if (
        comment
        and comment not in ignored_comments
    ):
        text += (
            f'\n💬 "{comment}"'
        )

    return text


# ============================================================
# CRÉATION DU CONSUMER REDPANDA
# ============================================================

def make_consumer():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "slack-notifier-v2",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe(
        [TOPIC]
    )

    return consumer


# ============================================================
# CONNEXION À REDPANDA
# ============================================================

while True:
    try:
        consumer = make_consumer()

        print(
            f"Slack consumer écoute "
            f"{TOPIC} sur {BOOTSTRAP}",
            flush=True,
        )

        break

    except Exception as exc:
        print(
            f"Redpanda indisponible ({exc}); "
            f"nouvel essai dans 5 s",
            flush=True,
        )

        time.sleep(5)


# ============================================================
# CONSOMMATION DES ÉVÉNEMENTS
# ============================================================

try:

    while True:

        record = consumer.poll(1.0)

        if record is None:
            continue

        if record.error():
            raise KafkaException(
                record.error()
            )

        try:
            # ------------------------------------------------
            # Lecture du JSON Debezium
            # ------------------------------------------------

            event = json.loads(
                record.value().decode("utf-8")
            )

            # Debezium produit :
            #
            # {
            #     "schema": {...},
            #     "payload": {
            #         "activity_id": ...,
            #         "employee_id": ...,
            #         ...
            #     }
            # }
            #
            # On récupère donc le payload.
            #
            # Le fallback permet également de fonctionner
            # si un événement arrive déjà sans enveloppe.
            activity = event.get(
                "payload",
                event,
            )

            # ------------------------------------------------
            # Sécurité : payload vide
            # ------------------------------------------------

            if not activity:
                print(
                    "Événement sans payload ignoré",
                    flush=True,
                )

                consumer.commit(record)
                continue

            activity_id = activity.get(
                "activity_id"
            )

            source = activity.get(
                "source"
            )

            print(
                f"Événement reçu : "
                f"activity_id={activity_id}, "
                f"source={source}",
                flush=True,
            )

            # ------------------------------------------------
            # Slack uniquement pour les activités LIVE
            # ------------------------------------------------

            if source != "live":

                consumer.commit(record)

                continue

            # ------------------------------------------------
            # Construction du message
            # ------------------------------------------------

            message = build_message(
                activity
            )

            # ------------------------------------------------
            # Envoi Slack
            # ------------------------------------------------

            if WEBHOOK:

                response = requests.post(
                    WEBHOOK,
                    json={
                        "text": message
                    },
                    timeout=10,
                )

                response.raise_for_status()

                print(
                    f"Slack envoyé pour "
                    f"activity_id={activity_id}",
                    flush=True,
                )

            else:

                print(
                    "[SLACK_WEBHOOK_URL vide] "
                    "Notification simulée:\n"
                    + message,
                    flush=True,
                )

            # On commit uniquement après traitement
            consumer.commit(record)

        except Exception as exc:

            print(
                f"Erreur traitement Slack : {exc}",
                flush=True,
            )


finally:

    consumer.close()