from pyspark.sql import SparkSession
def get_spark():
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("lucky-draw-pipeline") \
        .config("spark.jars", ",".join([
            "/opt/spark/jars/postgresql-42.7.3.jar",
            "/opt/spark/jars/delta-spark_2.12-3.1.0.jar",
            "/opt/spark/jars/delta-storage-3.1.0.jar"
        ])) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark