import os
from pyspark.sql import *
from pyspark.sql.functions import (
    date_format,
    col,
    current_timestamp,
    lit
)
from config.spark_session import get_spark
from config.settings import (
    BRONZE,
    RETAILER_DB,
    TRANSACTION_DB
)
spark = get_spark()
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

RETAILER_DB_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{RETAILER_DB}"
)

TRANSACTION_DB_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{TRANSACTION_DB}"
)

def read_postgres_table(db_url, table_name):
    return (
        spark.read
        .format("jdbc")
        .option("url", db_url)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

def write_bronze(df, table_name, ts_col="created_at"):
    (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("postgres"))
        .withColumn("year_month", date_format(col(ts_col), "yyyy-MM"))
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year_month")
        .option("overwriteSchema", "true")
        .save(f"{BRONZE}/{table_name}")
    )


def write_bronze_non_partitioned(df, table_name):
    (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("postgres"))
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{BRONZE}/{table_name}")
    )

retailers_df = read_postgres_table(
    RETAILER_DB_URL,
    "public.retailers"
)

lucky_draw_df = read_postgres_table(
    TRANSACTION_DB_URL,
    "public.farmer_lucky_draw"
)

inventory_df = read_postgres_table(
    TRANSACTION_DB_URL,
    "public.retailer_inventory"
)

ledger_df = read_postgres_table(
    TRANSACTION_DB_URL,
    "public.retailer_ledger"
)

write_bronze(lucky_draw_df, "lucky_draw")
write_bronze(ledger_df, "ledger")
write_bronze_non_partitioned(
    inventory_df,
    "inventory"
)
write_bronze_non_partitioned(
    retailers_df,
    "retailers"
)
