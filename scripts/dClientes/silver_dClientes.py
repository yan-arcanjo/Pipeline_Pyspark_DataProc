from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, upper, concat, lit, current_timestamp, from_utc_timestamp, date_format

APP_NAME = "silver_fClientes"

def get_spark_session():
    spark = SparkSession.builder \
                .appName(APP_NAME) \
                .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
    print("-- Spark Session Created")
    return spark

def read_bronze_data(spark, bronze_path):
    df_bronze = spark.read.format("delta").load(bronze_path).where("current_flag = true")
    print("-- DF Bronze Read")
    return df_bronze

def prepare_silver_df(df_bronze):
    dt_atualizacao = current_timestamp()
    df_silver = df_bronze.select(
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
        dt_atualizacao.alias("data_atualizacao")
    ).alias("bronze")

    print("-- DF Silver Prepared")

    return df_silver

def merge_silver_table(spark, silver_path, df_silver):
    dt_atualizacao = current_timestamp()

    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_delta = DeltaTable.forPath(spark, silver_path).alias("silver")
        
        silver_delta \
            .merge(
                df_silver, "silver.id_cliente = bronze.id_cliente"
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
                    "data_atualizacao": dt_atualizacao
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
                    "data_atualizacao": dt_atualizacao
                }
            ).whenNotMatchedBySourceDelete() \
            .execute()

        print("-- Merge done")
    else:   
        df.write.format("delta").mode("overwrite").save(silver_path)

def main():
    source_layer = "bronze"
    target_layer = "silver"
    table = "dClientes"

    bronze_path = f"gs://studies_dataproc/{source_layer}/{source_layer}_{table}"
    silver_path = f"gs://studies_dataproc/{target_layer}/{target_layer}_{table}"

    spark = get_spark_session()
    df_bronze = read_bronze_data(spark, bronze_path)
    df_silver = prepare_silver_df(df_bronze)
    merge_silver_table(spark, silver_path, df_silver)

    spark.stop()

if __name__ == "__main__":
    main()