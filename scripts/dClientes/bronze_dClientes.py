import logging
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, current_timestamp, lit
from datetime import datetime
import sys

APP_NAME = "bronze_fClientes"
layer_source = "raw"
layer_target = "bronze"
table = "dClientes"

# Configurando logging para monitoramento
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Variáveis de atualização
data_hora_script = datetime.now()    # Data hora atual
data_referencia_db = datetime.strftime("%Y-%m-%d %H:%M:%S")  # Formato banco de dados: YYYY-MM-DD HH:MM:SS

# Cria a Sessão Spark
spark = SparkSession.builder \
    .appName(APP_NAME) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logging.info("SparkSession inicializada com sucesso.")

# Define as variáveis com os caminhos dos arquivos
raw_path = f"gs://studies_dataproc/{layer_source}/{layer_source}_{table}/{layer_source}_{table}.parquet"
bronze_path = f"gs://studies_dataproc/{layer_target}/{layer_target}_{table}"

# Fazendo a leitura do arquivo parquet no GCS
raw_df = spark.read.parquet(raw_path) \
    .alias("raw")

logging.info(f"Arquivo {layer_source}_{table}.parquet lido.")

# Verifica se existe a tabela delta no bronze_path e faz o merge para manter histórico
if DeltaTable.isDeltaTable(spark, bronze_path):
    try:
        delta_table = DeltaTable.forPath(spark, bronze_path).alias("bronze")
    except Exception as e:
        logging.exception(f"Erro ao carregar {bronze_path}: {e}")
        sys.exit(1)
    
    logging.info("Iniciando atualização da delta bronze.")
    
    # Utilizando técnica SCD Type 2 para menter o histórico de todas as alterações
    # Compara a raw com os dados atuais na bronze, se algo mudou mudou então desativa a linha
    delta_table = DeltaTable.forPath(spark, bronze_path).alias("bronze")
    delta_table \
        .merge(raw_df, "raw.customer_id = bronze.customer_id and bronze.current_flag = True") \
        .whenMatchedUpdate(
            condition="""
                    raw.first_name <> bronze.first_name OR
                    raw.last_name <> bronze.last_name OR
                    raw.email <> bronze.email OR
                    raw.phone <> bronze.phone OR
                    raw.city <> bronze.city OR
                    raw.state <> bronze.state OR
                    raw.signup_date <> bronze.signup_date OR
                    raw.last_purchase_date <> bronze.last_purchase_date OR
                    raw.total_spent <> bronze.total_spent OR
                    raw.is_active <> bronze.is_active
                """, set = {"end_date": current_date(), 
                            "current_flag" : lit(False), 
                            "updated_at": current_timestamp()})\
        .execute()
    
    # Insere as linhas novas
    # Desativa se não existir mais na fonte
    delta_table \
        .merge(raw_df, "raw.customer_id = bronze.customer_id and bronze.current_flag = True") \
        .whenNotMatchedInsert(values={
                "customer_id": "raw.customer_id",
                "first_name": "raw.first_name",
                "last_name": "raw.last_name",
                "email": "raw.email",
                "phone": "raw.phone",
                "city": "raw.city",
                "state": "raw.state",
                "signup_date": "raw.signup_date",
                "last_purchase_date": "raw.last_purchase_date",
                "total_spent": "raw.total_spent",
                "is_active": "raw.is_active",
                "start_date": current_date(),
                "end_date": lit("9999-12-31").cast("date"),
                "current_flag":  lit(True),
                "updated_at": current_timestamp()       
        })\
        .whenNotMatchedBySourceUpdate(
            "bronze.current_flag = True", 
            set = {"end_date": current_date(), "current_flag" : lit(False), "updated_at": current_timestamp()})\
        .execute()
    
    logging.info("Finalizado atualização da delta bronze.")
else:
    # Cria a tabela delta com os dados iniciais caso ela não exista
    logging.info("Tabela delta não existe, sendo criado.")
    raw_df.write.format("delta").mode("overwrite").save(bronze_path)
    logging.info("Tabela delta criada com sucesso.")

spark.stop()