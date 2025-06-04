from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

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
            gcloud dataproc batches submit \
                --project mineral-seat-458614-s8 \
                --region us-east1 \
                pyspark \
                --batch processed_bronze_dClientes-{str(datetime.now()).replace("-", "").replace(" ", "-").replace(":","")[:15]} \
                gs://studies_dataproc/scripts/bronze_dClientes.py \
                --version 1.1 \
                --subnet default \
                --service-account 483261401319-compute@developer.gserviceaccount.com \
                --properties spark.jars.packages=io.delta:delta-core_2.12:2.2.0,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
        """
    )

    process_silver_layer = BashOperator(
        task_id="process_silver_layer",
        bash_command=f"""
            gcloud dataproc batches submit \
                --project mineral-seat-458614-s8 \
                --region us-east1 \
                pyspark \
                --batch processed_silver_dClientes-{str(datetime.now()).replace("-", "").replace(" ", "-").replace(":","")[:15]} \
                gs://studies_dataproc/scripts/silver_dClientes.py \
                --version 1.1 \
                --subnet default \
                --service-account 483261401319-compute@developer.gserviceaccount.com \
                --properties spark.jars.packages=io.delta:delta-core_2.12:2.2.0,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
        """
    )

    process_gold_layer = BashOperator(
        task_id="process_gold_layer",
        bash_command=f"""
            gcloud dataproc batches submit pyspark \
                --project mineral-seat-458614-s8 \
                --region us-east1 \
                --batch processed_gold_dClientes-{str(datetime.now()).replace("-", "").replace(" ", "-").replace(":","")[:15]} \
                gs://studies_dataproc/scripts/gold_dClientes.py \
                --version 1.1 \
                --subnet default \
                --service-account 483261401319-compute@developer.gserviceaccount.com \
                --properties spark.jars.packages=io.delta:delta-core_2.12:2.2.0,spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
        """
    )


    finish_pipeline = BashOperator(
        task_id = "finish",
        bash_command="echo 'Processamento finalizado...'"
    )

    start_pipeline >> process_bronze_layer >> process_silver_layer >> process_gold_layer >> finish_pipeline

