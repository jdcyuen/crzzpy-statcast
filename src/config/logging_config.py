import logging
import colorlog

ALLOWED_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET
}

def setup_logging(level_name: str, log_file=None):
    """
    Sets up logging to the console and optionally to a log file.
    
    Args:
        level (str): Logging level name (e.g., "INFO", "DEBUG").
        log_file (str or None): Path to the log file. If None, no file logging is enabled.
    """

    level_name_upper = level_name.upper()
    numeric_level = ALLOWED_LEVELS.get(level_name_upper, logging.INFO)

    # Define log format
    formatter = colorlog.ColoredFormatter(
        fmt="%(log_color)s[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red"
        }
    )
    
    handlers = []

    # Console handler with colors
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    

    # Optional file handler (no color)
    if log_file:
        file_formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)

    logging.basicConfig(level=numeric_level, handlers=handlers, force=True)

   

    