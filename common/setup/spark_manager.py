from pyspark.sql import SparkSession

def get_spark_session(app_name="UEBA_Plugin_Pipeline"):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark