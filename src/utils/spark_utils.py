from pyspark.sql import SparkSession

def get_spark_session(app_name):
    """
    Create or retrieve a Spark session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
