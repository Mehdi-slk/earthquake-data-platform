from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

@dag(
    dag_id="earthquake_data_etl",
    start_date=datetime(2026, 9, 21),
    schedule="@daily",
    catchup=True
)
def earthquake_data():

    @task
    def extract_produce_data():
        import subprocess
        from airflow.operators.python import get_current_context

        context = get_current_context()

        start_date = context["logical_date"].strftime("%Y-%m-%d")
        end_date = (context["logical_date"] + timedelta(days=1)).strftime("%Y-%m-%d")

        subprocess.run(
            [
                "python3",
                "/opt/airflow/producers/earthquake_producer.py",
                start_date,
                end_date
            ],
            check=True
        )

    transform_load_data = SparkSubmitOperator(
        task_id="transform_load_data",
        application="/opt/airflow/scripts/earthquake_data.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077"
        }
    )

    @task
    def build_and_fill_star_schema():
        import subprocess

        subprocess.run(
            [
                "docker",
                "exec",
                "dbt",
                "dbt",
                "build",
                "--project-dir",
                "/usr/app/earthquake_dbt"
            ],
            check=True
        )

    extract_produce_data() >> transform_load_data >> build_and_fill_star_schema()


earthquake_data()