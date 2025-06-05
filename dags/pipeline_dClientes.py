from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import pendulum
from datetime import datetime

default_args = {
    "owner": "yanarcanjo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=10),
}

with DAG(
    dag_id="pipeline_dClientes",
    catchup=False,
    schedule=None, # Trigger Manual
    tags = ["dataproc", "pyspark", "dClientes"],
    default_args = default_args,
    description="DAG que orquestra o pipeline de dados de clientes"
) as dag:

    start_pipeline = BashOperator(
        task_id = "start",
        bash_command="echo 'Iniciando o processamento...'"
    )

    process_bronze_layer = BashOperator(
        task_id="process_bronze_layer",
        bash_command=f"""
             gcloud dataproc jobs submit pyspark gs://studies_dataproc/scripts/Clientes/bronze_dClientes.py \
            --cluster=datalake \
            --region=us-central1 
        """
    )

    process_silver_layer = BashOperator(
        task_id="process_silver_layer",
        bash_command=f"""
             gcloud dataproc jobs submit pyspark gs://studies_dataproc/scripts/Clientes/silver_dClientes.py \
            --cluster=datalake \
            --region=us-central1 
        """
    )

    process_gold_layer = BashOperator(
        task_id="process_gold_layer",
        bash_command=f"""
           gcloud dataproc jobs submit pyspark gs://studies_dataproc/scripts/Clientes/gold_dClientes.py \
            --cluster=datalake \
            --region=us-central1 
        """
    )


    finish_pipeline = BashOperator(
        task_id = "finish",
        bash_command="echo 'Processamento finalizado...'"
    )

    start_pipeline >> process_bronze_layer >> process_silver_layer >> process_gold_layer >> finish_pipeline

