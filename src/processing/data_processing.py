from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def clean_data(df: DataFrame):
    """
    Process and clean data (e.g., null handling, filtering).
    """
    print("Processing data: Filtering nulls...")
    df = df.filter(col("AGE").isNotNull() & col("DISTRICT_NAME").isNotNull())
    return df
