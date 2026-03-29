import os
import logging
from logging.handlers import RotatingFileHandler

# --- CONFIGURATION ---
LOCK_FILE = "audit.lock"
DB_NAME = "fortress_history.db"

# Timeout settings
API_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_DELAY = 2

# Logging Configuration
LOG_FILE = "audit.log"

def setup_logging():
    """Configures production-grade logging."""
    logger = logging.getLogger("FortressAudit")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if setup called multiple times
    if not logger.handlers:
        # File Handler (Rotating)
        file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console Handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger

logger = setup_logging()
