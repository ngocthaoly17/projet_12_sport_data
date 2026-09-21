# Legacy

Le DAG Airflow de l'ancien POC est conservé uniquement comme trace de l'ancienne architecture batch.
Il n'est plus utilisé dans le chemin principal des données.

La nouvelle chaîne est événementielle : PostgreSQL -> Debezium -> Redpanda -> Spark/Slack.
