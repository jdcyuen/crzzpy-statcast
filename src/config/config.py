import yaml
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")

with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

# Statcast URLs
BASE_MLB_URL = CONFIG["statcast"]["base_urls"]["mlb"]
BASE_MiLB_URL = CONFIG["statcast"]["base_urls"]["milb"]

# Statcast Headers
MLB_HEADERS = CONFIG["statcast"]["headers"]["mlb"]
MiLB_HEADERS = CONFIG["statcast"]["headers"]["milb"]

# Statcast Params
PARAMS_DICT = CONFIG["statcast"]["params"]

# GCP Settings
GCP_PROJECT_ID = CONFIG["gcp"]["project_id"]
GCP_DATASET_ID = CONFIG["gcp"]["dataset_id"]
GCP_TABLE_PREFIX = CONFIG["gcp"]["table_prefix"]

# Logging Settings
LOG_LEVEL = CONFIG["logging"]["level"]
LOG_FILE = CONFIG["logging"]["log_file"]
CONSOLE_FORMAT = CONFIG["logging"]["console_format"]
FILE_FORMAT = CONFIG["logging"]["file_format"]
DATE_FORMAT = CONFIG["logging"]["date_format"]
LOG_COLORS = CONFIG["logging"]["log_colors"]
