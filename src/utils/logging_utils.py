import logging

def setup_logging(log_path, log_level="INFO"):
    """
    Set up logging configuration dynamically.
    """
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_path, mode="a"),
            logging.StreamHandler()  # Logs to console as well
        ],
    )
