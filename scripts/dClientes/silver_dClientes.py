import logging
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, upper, concat, lit, date_format
from datetime import datetime
import sys

# Definindo variáveis utilizadas
APP_NAME = "silver_fClientes"
layer_source = "bronze"
layer_target = "silver"
table = "dClientes"

# Configurando logging para monitoramento
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Variáveis de atualização
data_hora_script = datetime.now()    # Data hora atual
data_referencia_db = data_hora_script.strftime("%Y-%m-%d %H:%M:%S")  # Formato banco de dados: YYYY-MM-DD HH:MM:SS

# Cria a Sessão Spark
logging.info("Iniciando SparkSession.")

spark = SparkSession.builder \
    .appName(APP_NAME) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logging.info("SparkSession inicializada com sucesso.")

# Define as variáveis com os caminhos dos arquivos
bronze_path = f"gs://studies_dataproc/{layer_source}/{layer_source}_{table}"
silver_path = f"gs://studies_dataproc/{layer_target}/{layer_target}_{table}"

# Carrega a tabela delta bronze
try:
    bronze_df = DeltaTable.forPath(spark, bronze_path).toDF().where("current_flag = true")
    logging.info("Delta bronze carregado com sucesso.")
except Exception as e:
    logging.exception(f"Erro ao carregar {bronze_path}: {e}")
    sys.exit(1)    


# Processando tabela bronze, alterando nome e aplicando alguns tratamentos básicos
processed_df = bronze_df.select(
        col("customer_id").alias("id_cliente"),
        date_format(col("signup_date"), "yyyyMM").alias("ano_mes"), 
        col("first_name").alias("nome"),
        col("last_name").alias("sobrenome"),
        concat(col("first_name"), lit(" "), col("last_name")).alias("nome_completo"),
        col("email").alias("email"),
        col("phone").alias("telefone"),
        col("city").alias("cidade"),
        upper(col("state")).alias("estado"),
        col("signup_date").alias("data_entrada"),
        col("last_purchase_date").alias("data_ultima_compra"),
        col("total_spent").alias("total_gasto"), 
        col("is_active").alias("status"), 
        lit(data_referencia_db).alias("data_atualizacao")
    ).alias("bronze")

# Atualizando tabela silver utilizando merge
if DeltaTable.isDeltaTable(spark, silver_path):
    silver_delta = DeltaTable.forPath(spark, silver_path).alias("silver")

    logging.info("Carregado Delta Silver e iniciando atualização dos dados...")
    
    silver_delta \
        .merge(
            processed_df, "silver.id_cliente = bronze.id_cliente"
        ).whenMatchedUpdate(
            condition = """
                silver.nome <> bronze.nome OR
                silver.sobrenome <> bronze.sobrenome OR
                silver.nome_completo <> bronze.nome_completo OR
                silver.email <> bronze.email OR
                silver.telefone <> bronze.telefone OR
                silver.cidade <> bronze.cidade OR
                silver.estado <> bronze.estado OR
                silver.data_entrada <> bronze.data_entrada OR
                silver.data_ultima_compra <> bronze.data_ultima_compra OR
                silver.total_gasto <> bronze.total_gasto OR
                silver.status <> bronze.status
            """,
            set = {
                "nome": col("bronze.nome"),
                "ano_mes": col("bronze.ano_mes"),
                "sobrenome": col("bronze.sobrenome"),
                "nome_completo": col("bronze.nome_completo"),
                "email": col("bronze.email"),
                "telefone": col("bronze.telefone"),
                "cidade": col("bronze.cidade"),
                "estado": col("bronze.estado"),
                "data_entrada": col("bronze.data_entrada"),
                "data_ultima_compra": col("bronze.data_ultima_compra"),
                "total_gasto": col("bronze.total_gasto"),
                "status": col("bronze.status"),
                "data_atualizacao": lit(data_referencia_db)
            }
        ).whenNotMatchedInsert(
            values = {
                "id_cliente": col("bronze.id_cliente"),
                "ano_mes": col("bronze.ano_mes"),
                "nome": col("bronze.nome"),
                "sobrenome": col("bronze.sobrenome"),
                "nome_completo": col("bronze.nome_completo"),
                "email": col("bronze.email"),
                "telefone": col("bronze.telefone"),
                "cidade": col("bronze.cidade"),
                "estado": col("bronze.estado"),
                "data_entrada": col("bronze.data_entrada"),
                "data_ultima_compra": col("bronze.data_ultima_compra"),
                "total_gasto": col("bronze.total_gasto"),
                "status": col("bronze.status"),
                "data_atualizacao": lit(data_referencia_db)
            }
        ).whenNotMatchedBySourceDelete() \
        .execute()

    logging.info("Delta Silver atualizada.")
else:   
    # Caso não exista a tabela silver, será criado
    processed_df.write.format("delta").mode("overwrite").save(silver_path)

spark.stop()