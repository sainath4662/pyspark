import argparse
import yaml
import logging
from utils.logging_utils import setup_logging
from utils.spark_utils import get_spark_session
from ingestion.data_ingestion import read_data
from processing.data_processing import clean_data
from transformations.data_transform import transform_data
from writer.data_writer import write_data

if __name__ == "__main__":
    # Step 0: Parse environment argument
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="default", help="Environment: default/dev/prod")
    args = parser.parse_args()

    # Step 1: Load configuration
    with open("D:/pyspark/Project/config/config.yaml") as f:
        config = yaml.safe_load(f)[args.env]

    # Step 2: Set up logging
    log_path = config["log_path"]
    log_level = config.get("log_level", "INFO")
    setup_logging(log_path, log_level)
    logger = logging.getLogger(__name__)

    logger.info("Starting PySpark job with environment: %s", args.env)

    try:
        # Step 3: Initialize Spark
        spark = get_spark_session("PySpark Project")
        logger.info("Spark session initialized")

        # Step 4: Ingest data
        logger.info("Reading data from: %s", config["input_path"])
        df_raw = read_data(spark, config["input_path"])

        # Step 5: Clean data
        logger.info("Cleaning data")
        df_clean = clean_data(df_raw)

        # Step 6: Transform data
        logger.info("Transforming data")
        df_transformed = transform_data(df_clean)

        # Step 7: Write data
        logger.info("Writing data to: %s", config["output_path"])
        write_data(df_transformed, config["output_path"])

        logger.info("PySpark job completed successfully")

    except Exception as e:
        logger.error("An error occurred during the PySpark job", exc_info=True)

    finally:
        # Step 8: Stop Spark
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")
