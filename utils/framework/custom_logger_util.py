import logging
import os
from datetime import datetime
from pathlib import Path

LOGGER = None  # Global variable to store logger


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Return a logger with the specified name.

    Args:
        name (str): Name of the logger, default is the module's __name__.

    Returns:
        logging.Logger: Configured logger instance.
    """
    return logging.getLogger(name)


def setup_logging(config: any):

    global LOGGER  # Ensure we are modifying the global LOGGER variable

    if not hasattr(config, "workerinput"):
        logs_dir = Path(config.rootdir) / "logs" / datetime.now().strftime("%Y%m%d_%H%M%S")
        logs_dir.mkdir(parents=True, exist_ok=True)
        os.environ["LOGS_DIR"] = str(logs_dir)
    else:
        logs_dir = Path(os.environ["LOGS_DIR"])

    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    log_file = logs_dir / f"tests_{worker_id}.log" if worker_id else logs_dir / "tests.log"

    # Configure logging
    logging.basicConfig(
        format=config.getini("log_file_format"),
        filename=str(log_file),
        level=config.getini("log_file_level"),
    )

    # Create and configure logger
    LOGGER = logging.getLogger("pytest_session_logger")
    LOGGER.setLevel(logging.DEBUG)

    # Avoid duplicate handlers
    if not LOGGER.handlers:
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)

    config.logs_dir = str(logs_dir)
    return LOGGER  # Return logger instance for explicit passing
