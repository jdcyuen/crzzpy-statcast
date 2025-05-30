import argparse
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import os

import time
from progress.bar import ChargingBar

import requests
# from pybaseball import statcast


# Base URLs for MLB and MiLB
BASE_MLB_URL = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details"
BASE_MiLB_URL = "https://baseballsavant.mlb.com/statcast-search-minors/csv?all=true&type=details&minors=true"

MLB_HEADERS = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://baseballsavant.mlb.com/statcast_search",
}

MiLB_HEADERS = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://baseballsavant.mlb.com/statcast-search-minors",
}

PARAMS_DICT = {
            "all": "true",
            "hfPT": "",
            "hfAB": "",
            "hfGT": "",
            "hfPR": "",
            "hfZ": "",
            "stadium": "",
            "hfBBL": "",
            "hfNewZones": "",
            "hfPull": "",
            "hfC": "",
            "hfSea": "",
            "hfSit": "",
            "player_type": "batter",
            "hfOuts": "",
            "opponent": "",
            "pitcher_throws": "",
            "batter_stands": "",
            "hfSA": "",
'''
            "game_date_gt": day_start,
            "game_date_lt": day_end,
'''
            "team": "",
            "position": "",
            "hfRO": "",
            "home_road": "",
            "hfFlag": "",
            "metric_1": "",
            "hfInn": "",
            "min_pitches": "0",
            "min_results": "0",
            "group_by": "name",
            "sort_col": "game_date",
            "player_event_sort": "h_launch_speed",
            "sort_order": "desc",
            "min_pas": "0",
            "type": "details"
}

'''
OPTIONS = (
        f"&hfPT=&hfAB=&hfBB=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&"
        f"hfGT=R%7CPO%7CS%7C&hfC=&hfSea=&hfSit=&player_type=batter&hfOuts=&opponent=&"
        f"pitcher_throws=&batter_stands=&hfSA=&"
        f"team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&"
        f"group_by=day&sort_col=game_date&player_event_sort=api_p_release_speed&sort_order=desc"
        f"&pitch_type=&game_date=&release_speed=&release_pos_x=&release_pos_z="
)
'''  


def _daterange(start_date, end_date, delta_days):
    current = start_date
    while current < end_date:
        next_date = min(current + timedelta(days=delta_days), end_date)
        yield current, next_date
        current = next_date

def _fetch_chunk(start_date, end_date, base_url):
    url = f"{base_url}?all=true&&game_date_gt={start_date}&game_date_lt={end_date}"
    try:
        df = pd.read_csv(url)
        print(f"Fetched data from {start_date} to {end_date} ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"Failed to fetch data from {start_date} to {end_date}: {e}")
        return pd.DataFrame()



def _fetch_data_in_parallel(start_date, end_date, file_name, base_url, chunk_size=7, step_days=None, max_workers=4):
    pass  
    

def fetch_savant_data(start_date, end_date, base_url, headers, file_name="statCast_2025_all.csv"):


    bar = ChargingBar('Processing', max=20)

    # Append date parameters to the base URL
    full_url = f"{base_url}&game_date_gt={start_date}&game_date_lt={end_date}"
    print(full_url)
    response = requests.get(full_url, headers=headers, timeout=180)


    response.raise_for_status()

    # Read the data into a DataFrame
    df = pd.read_csv(StringIO(response.text))

    for i in range(20):
        # Do some work
        time.sleep(0.5)
    
        bar.next()

    # Save to CSV
    df.to_csv(file_name, index=False)
    print(f"  Data saved to {file_name}", end="") 
    bar.finish()

def count_rows_in_csv(file_name):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, file_name)

    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        row_count = sum(1 for _ in reader)
    print(f"  Total number of rows in '{file_name}': {row_count}",  end="\n")   


def main():
    """Main function to handle command-line arguments."""
    parser = argparse.ArgumentParser(description="Download Statcast data for a given date range.")
    parser.add_argument("start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--league", choices=["mlb", "milb", "both"], default="mlb",
                        help="Which league data to download: 'mlb', 'milb', or 'both'")
    parser.add_argument("--file", type=str, help="Output CSV file name (default: statcast_data.csv)")
    parser.add_argument("--chunk_size", type=int, default=7, help="Days per chunk")
    parser.add_argument("--step_days", type=int, help="Optional custom step between chunks")
    parser.add_argument("--max_workers", type=int, default=4, help="Number of parallel threads")

    args = parser.parse_args()

    if args.league == "mlb":
        fetch_savant_data(args.start_date, args.end_date, BASE_MLB_URL, MLB_HEADERS, "statcast_mlb.csv")
        count = count_rows_in_csv("statcast_mlb.csv")

    elif args.league == "milb":
         fetch_savant_data(args.start_date, args.end_date, BASE_MiLB_URL, MiLB_HEADERS, "statcast_milb.csv")
         count = count_rows_in_csv("statcast_milb.csv")

    elif args.league == "both":
        fetch_savant_data(args.start_date, args.end_date, BASE_MLB_URL, MLB_HEADERS, "statcast_mlb.csv")
        count = count_rows_in_csv("statcast_mlb.csv")

        fetch_savant_data(args.start_date, args.end_date, BASE_MiLB_URL, MiLB_HEADERS, "statcast_milb.csv")
        count = count_rows_in_csv("statcast_milb.csv")


if __name__ == "__main__":
    main()
