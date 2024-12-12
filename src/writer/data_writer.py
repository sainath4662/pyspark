from pyspark.sql import DataFrame

def write_data(df: DataFrame, output_path: str):
    """
    Write processed data to the specified path.
    """
    print(f"Writing data to: {output_path}")
    df.write.mode("overwrite").csv(output_path, header=True)
