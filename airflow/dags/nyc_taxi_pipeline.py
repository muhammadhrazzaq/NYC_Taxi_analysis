from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

SPARK_SUBMIT = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "src/nyc_taxi_analysis/jobs/{job}.py"
)

with DAG(
    dag_id="nyc_taxi_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["nyc-taxi", "lakehouse"],
) as dag:

    bronze = BashOperator(task_id="ingest_bronze",   bash_command=SPARK_SUBMIT.format(job="ingest_bronze"))
    silver = BashOperator(task_id="silver_trips",    bash_command=SPARK_SUBMIT.format(job="silver_trips"))
    gold   = BashOperator(task_id="gold_obt",        bash_command=SPARK_SUBMIT.format(job="gold_obt"))
    aggs   = BashOperator(task_id="gold_aggregates", bash_command=SPARK_SUBMIT.format(job="gold_aggregates"))

    bronze >> silver >> gold >> aggs