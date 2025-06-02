from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F

def main():

    layer_source = "raw"
    layer_target = "bronze"
    table = "dClientes"
    raw_path = f"gs://studies_dataproc/{layer_source}/{layer_source}_{table}"
    bronze_path = f"gs://studies_dataproc/{layer_target}/{layer_target}_{table}"


    spark = SparkSession.builder \
        .appName("bronze_dClientes") \
        .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

    layer_source = "raw"
    layer_target = "bronze"
    table = "dClientes"
    bronze_path = f"gs://studies_dataproc/{layer_target}/{layer_target}_{table}"


    df = spark.read.parquet(raw_path) \
        .alias("raw")

    if DeltaTable.isDeltaTable(spark, bronze_path):
        print("-- Iniciando o merge...")
        delta_table = DeltaTable.forPath(spark, bronze_path).alias("bronze")
        delta_table \
            .merge(df, "raw.customer_id = bronze.customer_id and bronze.current_flag = True") \
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
                    """, set = {"end_date": F.current_date(), 
                                "current_flag" : F.lit(False), 
                                "updated_at": F.current_timestamp()})\
            .execute()
        
        delta_table \
            .merge(df, "raw.customer_id = bronze.customer_id and bronze.current_flag = True") \
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
                    "start_date": F.current_date(),
                    "end_date": F.lit("9999-12-31").cast("date"),
                    "current_flag":  F.lit(True),
                    "updated_at": F.current_timestamp()       
            })\
            .whenNotMatchedBySourceUpdate(
                "bronze.current_flag = True", 
                set = {"end_date": F.current_date(), "current_flag" : F.lit(False), "updated_at": F.current_timestamp()})\
            .execute()
        
        print("-- Merge finalizado...")
    else:
        print("-- Criando tabela delta...")
        df.write.format("delta").mode("overwrite").save(bronze_path)
        print("-- Criação finalizada...")

    spark.stop()

if __name__ == "__main__":
    main()