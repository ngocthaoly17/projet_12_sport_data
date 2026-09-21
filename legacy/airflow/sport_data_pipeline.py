from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="sport_data_pipeline",
    description="Orchestration du pipeline Sport Data Solution",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sport-data", "poc"],
) as dag:

    check_environment = BashOperator(
        task_id="check_environment",
        bash_command="""
        echo "Airflow fonctionne"
        echo "Date d'exécution : $(date)"
        echo "Première étape du pipeline Sport Data"
        """,
    )

    check_environment