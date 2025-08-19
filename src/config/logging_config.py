import logging
import colorlog
import os
from src.config.config import LOG_LEVEL, LOG_FILE, CONSOLE_FORMAT, FILE_FORMAT, DATE_FORMAT, LOG_COLORS

ALLOWED_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET
}

def setup_logging(level_name: str = None, log_file: str = None):
    """
    Sets up logging to the console and optionally to a log file.

    Args:
        level_name (str): Logging level name (e.g., "INFO", "DEBUG").
        log_file (str or None): Path to the log file. If None, defaults to logs/statcast.log.
    """
    #print(f"Initial log_file arg: {log_file}")

    level_str = level_name or LOG_LEVEL
    #log_file_path = log_file or LOG_FILE

    numeric_level = ALLOWED_LEVELS.get(level_str.upper(), logging.INFO)

    # Default log file if not provided
    if log_file is None:
        log_file = os.path.join("logs", "statcast.log")
    else:
        # If user provided just a filename (no directory), prepend logs/
        if os.path.dirname(log_file) == '':
            log_file = os.path.join("logs", log_file)

    #print(f"Using log file: {log_file}")

    # Ensure directory exists if there's a folder in the path
    log_dir = os.path.dirname(log_file)
    #print(f"Directory to create: '{log_dir}'")

    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        #print(f"Created logs directory '{log_dir}'")

    # Define log format
    formatter = colorlog.ColoredFormatter(
        fmt=CONSOLE_FORMAT,
        datefmt=DATE_FORMAT,
        log_colors=LOG_COLORS
    )
    
    handlers = []

    # Console handler with colors
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    

    # Optional file handler (no color)
    if log_file:
        #os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        file_formatter = logging.Formatter(fmt=FILE_FORMAT, datefmt=DATE_FORMAT)
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)

    logging.basicConfig(level=numeric_level, handlers=handlers, force=True)

   

    