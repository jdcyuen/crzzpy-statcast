import logging
import sys
import difflib
import colorlog

ALLOWED_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET
}

def setup_logging(level_name: str):
    level_name_upper = level_name.upper()
    if level_name_upper not in ALLOWED_LEVELS:
        print(f"❌ Invalid log level: '{level_name}'")
        suggestions = difflib.get_close_matches(level_name_upper, ALLOWED_LEVELS.keys(), n=1)
        if suggestions:
            print(f"💡 Did you mean: '{suggestions[0].lower()}'?")
        print("✅ Allowed values: " + ", ".join(l.lower() for l in ALLOWED_LEVELS))
        sys.exit(1)

    # Set up colorlog handler and formatter
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        fmt="%(log_color)s[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'bold_red',
        }
    ))

    # Clear root logger handlers and apply colorlog setup
    root_logger = colorlog.getLogger()
    root_logger.setLevel(ALLOWED_LEVELS[level_name_upper])
    root_logger.handlers = []  # Clear existing handlers
    root_logger.addHandler(handler)
