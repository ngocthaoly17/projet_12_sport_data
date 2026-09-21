# Architecture cible validée avec le mentor

Correspondance entre le schéma de référence et ce POC :

| Schéma mentor | Implémentation du POC |
|---|---|
| Strava-like generation | `app/main.py` + `scripts/generate_live_activity.py` |
| Data System | PostgreSQL `raw.activities` |
| Log System / différentiel | WAL logique PostgreSQL |
| Debezium | service `debezium` + connecteur `activities-cdc` |
| Redpanda topic | `sport_data.raw.activities` |
| Python -> Slack | `streaming/slack_consumer.py` |
| Spark #1 | `spark/bronze_stream.py` |
| Delta Lake #1 | `s3://lakehouse/bronze/activities` |
| Référentiel entreprise | `s3://references/company/employees.csv` |
| Référentiel sportif | `s3://references/sports/employee_sports.csv` |
| Spark #2 | `spark/silver_stream.py` |
| Delta Lake #2 | `s3://lakehouse/silver/employee_activities` |
| Power BI | couche Silver ; export local dans `powerbi/output/` |

Le point important est que la source Python n'appelle ni Spark, ni Slack, ni Redpanda. Elle écrit uniquement dans PostgreSQL. Le reste du pipeline est déclenché par le changement capturé dans le WAL.
