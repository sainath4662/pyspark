from pyspark.sql import DataFrame
from pyspark.sql.functions import count

def transform_data(df: DataFrame):
    """
    Example transformation: Aggregate data by district.
    """
    print("Transforming data: Aggregating by DISTRICT_NAME...")
    return df.groupBy("DISTRICT_NAME").agg(count("*").alias("record_count"))
