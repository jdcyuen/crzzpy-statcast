
BASE_MLB_URL = "https://baseballsavant.mlb.com/statcast_search/csv"
BASE_MiLB_URL = "https://baseballsavant.mlb.com/statcast-search-minors/csv"

MLB_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://baseballsavant.mlb.com/statcast_search",
    "Connection": "close"
}

MiLB_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://baseballsavant.mlb.com/statcast-search-minors",
    "Connection": "close"
}

PARAMS_DICT = {
    "all": "true",
    "type": "details"
}