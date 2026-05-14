import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from config.spark_session import get_spark
from config.settings import (
    BRONZE,
    SILVER,
    GOLD
)

spark = get_spark()

retailers_df = spark.read.format("delta").load(f"{BRONZE}/retailers")

lucky_draw_df = spark.read.format("delta").load(f"{BRONZE}/lucky_draw")

inventory_df = spark.read.format("delta").load(f"{BRONZE}/inventory")

ledger_df = spark.read.format("delta").load(f"{BRONZE}/ledger")

retailer_geo = retailers_df.select(
    "buyer_account_id",
    "state",
    "district",
    "sub_district",
    "sales_region",
    "zone",
    "mdt_hq",
    "status",
    "erp_code"
).withColumn(
    "zone",
    F.when(
        F.col("state") == "West Bengal",
        "East"
    ).otherwise(F.col("zone"))
).withColumn(
    "zone",
    F.when(
        F.col("state") == "Karnataka",
        "South 2"
    ).otherwise(F.col("zone"))
).filter(
    F.col("state").isNotNull() &
    (F.trim(F.col("state")) != "")
)

lucky_draw_df = lucky_draw_df.filter(
    (col("quantity") > 0) &
    (col("quantity") < 10000)
)

lucky_draw_geo = lucky_draw_df.join(
    F.broadcast(retailer_geo),
    lucky_draw_df.retailer_account_id == retailer_geo.buyer_account_id,
    how="left"
).drop("buyer_account_id")

lucky_draw_geo = lucky_draw_geo \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("farmer_name", trim(col("farmer_name")))

state_window = Window.partitionBy("state")

lucky_draw_enriched = lucky_draw_geo \
    .withColumn(
        "state_total_quantity",
        F.sum("quantity").over(state_window)
    ) \
    .withColumn(
        "state_total_entries",
        F.count("id").over(state_window)
    ) \
    .withColumn(
        "state_rate",
        F.round(
            F.col("state_total_quantity") /
            F.col("state_total_entries"),
            2
        )
    ) \
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

farmer_window = Window.partitionBy("farmer_mobile")

farmer_time_window = Window.partitionBy(
    "farmer_mobile"
).orderBy("created_at")

farmer_profiles = lucky_draw_enriched \
    .withColumn(
        "total_entries",
        F.count("id").over(farmer_window)
    ) \
    .withColumn(
        "total_quantity",
        F.sum("quantity").over(farmer_window)
    ) \
    .withColumn(
        "first_entry_date",
        F.min("created_at").over(farmer_window)
    ) \
    .withColumn(
        "last_entry_date",
        F.max("created_at").over(farmer_window)
    ) \
    .withColumn(
        "unique_retailers",
        F.approx_count_distinct(
            "retailer_account_id"
        ).over(farmer_window)
    ) \
    .withColumn(
        "entry_rank_asc",
        F.row_number().over(farmer_time_window)
    ) \
    .withColumn(
        "entry_rank_desc",
        F.row_number().over(
            Window.partitionBy("farmer_mobile")
            .orderBy(F.col("created_at").desc())
        )
    ) \
    .dropDuplicates(["farmer_mobile"]) \
    .select(
        "farmer_mobile",
        "farmer_name",
        "state",
        "district",
        "total_entries",
        "total_quantity",
        "first_entry_date",
        "last_entry_date",
        "unique_retailers",
        "entry_rank_asc",
        "entry_rank_desc"
    )

ledger_window = Window.partitionBy(
    "retailer_account_id"
).orderBy("created_at")

ledger_unbounded = Window.partitionBy(
    "retailer_account_id"
).orderBy(
    "created_at"
).rowsBetween(
    Window.unboundedPreceding,
    0
)

ledger_enriched = ledger_df \
    .withColumn(
        "prev_balance",
        F.lag("balance_after", 1).over(ledger_window)
    ) \
    .withColumn(
        "next_balance",
        F.lead("balance_after", 1).over(ledger_window)
    ) \
    .withColumn(
        "movement_direction",
        F.when(F.col("quantity_change") > 0, "IN")
        .when(F.col("quantity_change") < 0, "OUT")
        .otherwise("ADJUSTMENT")
    ) \
    .withColumn(
        "days_since_last_movement",
        F.datediff(
            F.col("created_at"),
            F.lag("created_at", 1).over(ledger_window)
        )
    ) \
    .withColumn(
        "running_stock_in",
        F.sum(
            F.when(
                F.col("quantity_change") > 0,
                F.col("quantity_change")
            ).otherwise(0)
        ).over(ledger_unbounded)
    ) \
    .withColumn(
        "running_stock_out",
        F.sum(
            F.when(
                F.col("quantity_change") < 0,
                F.abs(F.col("quantity_change"))
            ).otherwise(0)
        ).over(ledger_unbounded)
    ) \
    .withColumn(
        "movement_number",
        F.row_number().over(ledger_window)
    ) \
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

ledger_enriched = ledger_enriched \
    .withColumn(
        "quantity_change",
        col("quantity_change").cast("int")
    ) \
    .withColumn(
        "balance_after",
        col("balance_after").cast("int")
    ) \
    .withColumn(
        "prev_balance",
        col("prev_balance").cast("int")
    ) \
    .withColumn(
        "next_balance",
        col("next_balance").cast("int")
    ) \
    .withColumn(
        "running_stock_in",
        col("running_stock_in").cast("int")
    ) \
    .withColumn(
        "running_stock_out",
        col("running_stock_out").cast("int")
    )

def write_silver(df, name):

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER}/{name}")
    )

    print(f"{name}: {df.count()} rows")

write_silver(
    lucky_draw_enriched,
    "lucky_draw_enriched"
)

write_silver(
    farmer_profiles,
    "farmer_profiles"
)

write_silver(
    ledger_enriched,
    "ledger_enriched"
)

targets = [
    ("lucky_draw_enriched", "retailer_account_id, state"),
    ("farmer_profiles", "farmer_mobile"),
    ("ledger_enriched", "retailer_account_id"),
]

for table, zcols in targets:

    spark.sql(f"""
        OPTIMIZE delta.`{SILVER}/{table}`
        ZORDER BY ({zcols})
    """)


lucky_draw = spark.read.format("delta").load(
    f"{SILVER}/lucky_draw_enriched"
)

farmers = spark.read.format("delta").load(
    f"{SILVER}/farmer_profiles"
)

ledger = spark.read.format("delta").load(
    f"{SILVER}/ledger_enriched"
)

retailers = spark.read.format("delta").load(
    f"{BRONZE}/retailers"
)


date_range = lucky_draw.agg(
    F.min(F.to_date("created_at")).alias("min_date"),
    F.max(F.to_date("created_at")).alias("max_date")
).collect()[0]

min_date = date_range["min_date"]
max_date = date_range["max_date"]

fact_lucky_draw = lucky_draw \
    .withColumn(
        "entry_date",
        F.to_date("created_at")
    ) \
    .withColumn(
        "entry_hour",
        F.hour("created_at")
    ) \
    .withColumn(
        "year_month",
        F.date_format("created_at", "yyyy-MM")
    ) \
    .withColumn(
        "campaign_day",
        F.datediff(
            F.to_date("created_at"),
            F.lit(min_date)
        ) + 1
    ) \
    .select(
        F.col("id").alias("entry_id"),
        "ticket_number",
        "farmer_mobile",
        F.col("retailer_account_id").alias("retailer_id"),
        "entry_date",
        "entry_hour",
        "year_month",
        "campaign_day",
        "quantity",
        "state_rate",
        "state",
        "district",
        "zone",
        "sales_region",
        "inventory_ledger_id"
    )

date_list = [
    (min_date + datetime.timedelta(days=x),)
    for x in range((max_date - min_date).days + 1)
]

dim_date = spark.createDataFrame(date_list, ["date"]) \
    .withColumn("year", F.year("date")) \
    .withColumn("month", F.month("date")) \
    .withColumn("quarter", F.quarter("date")) \
    .withColumn("week_of_year", F.weekofyear("date")) \
    .withColumn("day_of_week", F.dayofweek("date")) \
    .withColumn("day_name", F.date_format("date", "EEEE")) \
    .withColumn("month_name", F.date_format("date", "MMMM")) \
    .withColumn("year_month", F.date_format("date", "yyyy-MM")) \
    .withColumn(
        "is_weekend",
        F.when(
            F.dayofweek("date").isin([1, 7]),
            True
        ).otherwise(False)
    ) \
    .withColumn(
        "campaign_day",
        F.datediff(F.col("date"), F.lit(min_date)) + 1
    )

dim_retailer = retailers.select(
    F.col("buyer_account_id").alias("retailer_id"),
    "erp_code",
    "legal_name",
    "state",
    "district",
    "sub_district",
    "sales_region",
    "zone",
    "mdt_hq",
    "status",
    F.concat_ws(
        " ",
        F.col("first_name"),
        F.col("last_name")
    ).alias("retailer_full_name")
)

agg_retailer_leaderboard = fact_lucky_draw \
    .groupBy(
        "retailer_id",
        "state",
        "zone",
        "sales_region",
        "district"
    ) \
    .agg(
        F.count("entry_id").alias("total_entries"),
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("farmer_mobile").alias("unique_farmers"),
        F.min("entry_date").alias("first_entry_date"),
        F.max("entry_date").alias("last_entry_date")
    ) \
    .join(
        dim_retailer.select(
            "retailer_id",
            "legal_name",
            "retailer_full_name",
            "erp_code"
        ),
        "retailer_id",
        "left"
    ) \
    .withColumn(
        "rank_in_zone",
        F.rank().over(
            Window.partitionBy("zone")
            .orderBy(F.col("total_entries").desc())
        )
    ) \
    .withColumn(
        "rank_in_state",
        F.rank().over(
            Window.partitionBy("state")
            .orderBy(F.col("total_entries").desc())
        )
    ) \
    .withColumn(
        "rank_overall",
        F.rank().over(
            Window.orderBy(F.col("total_entries").desc())
        )
    )

gold_tables = {
    "fact_lucky_draw_entries": fact_lucky_draw,
    "dim_date": dim_date,
    "dim_retailer": dim_retailer,
    "agg_retailer_leaderboard": agg_retailer_leaderboard,
}

for name, df in gold_tables.items():
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD}/{name}")
    )
    print(f"{name}: {df.count()} rows")

