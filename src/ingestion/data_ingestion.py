from pyspark.sql import SparkSession

def read_data(spark: SparkSession, input_path: str):
    """
    Read raw data from the specified path.
    """
    print(f"Reading data from: {input_path}")
    return spark.read.option("header", True).option("inferSchema",True).csv(input_path)
