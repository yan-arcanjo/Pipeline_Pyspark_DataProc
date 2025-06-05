import logging
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, when, datediff, current_date
from datetime import datetime
import sys

# Definindo variáveis utilizadas
APP_NAME = "gold_fClientes"
layer_source = "silver"
layer_target = "gold"
table = "dClientes"
project_bq = "mineral-seat-458614-s8"
dataset_bq = "Clientes"

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

#Caminhos das tabelas delta
silver_path = "gs://studies_dataproc/silver/silver_dClientes"
gold_path = "gs://studies_dataproc/gold/gold_dClientes"

# Carregamento dos dados da camada Silver
try:
    silver_delta = DeltaTable.forPath(spark, silver_path).toDF().select(
        col("id_cliente"),
        col("ano_mes"),
        col("nome"),
        col("sobrenome"),
        col("nome_completo"),
        col("email"),
        col("telefone"),
        col("cidade"),
        col("estado"),
        col("data_entrada"),
        col("data_ultima_compra"),
        col("total_gasto"),
        col("status")
    )
except Exception as e:
    logging.exception(f"Erro ao carregar {silver_path}: {e}")
    sys.exit(1)   

logging.info("Tabela silver carregada.")

# Aplicando regras de negócio ao dataframe
# Nivel do cliente pelo total que ele já gastou
# Se é um cliente novo, Se fez alguma compra recente
# Quanto tempo estão sem comprar
# Nivel do risco de churn
enriched_df = silver_delta \
    .withColumn(
        "nivel_cliente",
        when(col("total_gasto") >= 10000, "Ouro")\
        .when(col("total_gasto") >= 5000, "Prata")\
        .when(col("total_gasto") >= 1000, "Bronze")\
        .otherwise("Novo")
    ) \
    .withColumn(
        "cliente_novo",
        when(datediff(current_date(), col("data_entrada")) <= 90, True) \
        .otherwise(False)
    ) \
    .withColumn(
        "compra_recente",
         when(datediff(current_date(), col("data_ultima_compra")) <= 15, True) \
        .otherwise(False)
    ) \
    .withColumn(
        "dias_sem_comprar",
         datediff(current_date(), col("data_ultima_compra")) 
    ) \
    .withColumn(
        "risco_churn",
         when(datediff(current_date(), col("data_ultima_compra")) > 180, "Alto") \
        .when(datediff(current_date(), col("data_ultima_compra")) > 90, "Médio" )\
        .otherwise("Baixo")
    )
logging.info("Regras de negócio criadas.")

# Salvando a tabela gold em uma tabela física no BigQuery
enriched_df.write\
    .format("bigquery")\
    .option("table", f"{project_bq}.{dataset_bq}.dClientes")\
    .mode("overwrite")\
    .option("writeMethod", "direct")\
    .save()

logging.info("Salvo tabela gold no BQ.")
# Salvando a tabela gold em uma tabela delta
enriched_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(gold_path)

logging.info("Salvo tabela gold em Delta.")

logging.info(f"Finalizado o processamento da tabela e gravado no BigQuery em f{project_bq}.{dataset_bq}.dClientes")
# CRIANDO ANÁLISES ESPECÍFICAS DE CLIENTES

logging.info("Iniciando análises específicas")
#       ANÁLISE DOS MELHORES E PIORES CLIENTES       #
# Top 10 clientes ativos com os maiores gastos
df_top_10_clientes = enriched_df.select(
    col("id_cliente"),
    col("nome_completo"),
    col("total_gasto"),
    col("status"),
    col("nivel_cliente"),
    col("dias_sem_comprar"),
    lit("Melhores clientes").alias("categoria"),
    lit(data_referencia_db).alias("data_hora_analise")
).filter(col("status") == True) \
    .orderBy(col("total_gasto").desc()) \
    .limit(10)

# Top 10 clientes ativos com os menores gastos
df_top_10_piores_clientes = enriched_df.select(
    col("id_cliente"),
    col("nome_completo"),
    col("total_gasto"),
    col("status"),
    col("nivel_cliente"),
    col("dias_sem_comprar"),
    lit("Clientes a serem trabalhados").alias("categoria"),
    lit(data_referencia_db).alias("data_hora_analise")
).filter(col("status") == "true") \
    .orderBy(col("total_gasto").asc()) \
    .limit(10)

# Unifica os 2 dataframes
df_melhores_piores = df_top_10_clientes.unionAll(df_top_10_piores_clientes)

# Salvando a tabela de análise em uma tabela física no BigQuery
df_melhores_piores.write\
    .format("bigquery")\
    .option("table", f"{project_bq}.{dataset_bq}.ranking_clientes")\
    .mode("overwrite")\
    .option("writeMethod", "direct")\
    .save()

logging.info("Gravado análises no BigQuery")
spark.stop()