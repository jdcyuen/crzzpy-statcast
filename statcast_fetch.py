import argparse
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from pybaseball import statcast


# Base URLs for MLB and MiLB
BASE_MLB_URL = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details"
BASE_MiLB_URL = "https://baseballsavant.mlb.com/statcast-search-minors/csv?all=true&type=details&minors=true"
HEADERS = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://baseballsavant.mlb.com/statcast_search",
    }


'''
    url = (
        f"https://baseballsavant.mlb.com/statcast_search/csv?"
        f"all=true&hfPT=&hfAB=&hfBB=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&"
        f"hfGT=R%7CPO%7CS%7C&hfC=&hfSea=&hfSit=&player_type=batter&hfOuts=&opponent=&"
        f"pitcher_throws=&batter_stands=&hfSA=&game_date_gt={start_date}&game_date_lt={end_date}&"
        f"team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&"
        f"group_by=day&sort_col=game_date&player_event_sort=api_p_release_speed&sort_order=desc"
        f"&pitch_type=&game_date=&release_speed=&release_pos_x=&release_pos_z="
        f"&type=details"
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
    
    

def fetch_savant_data(start_date, end_date, base_url, headers, file_name="statCast_2025_all.csv"):

    # Append date parameters to the base URL
    full_url = f"{base_url}&game_date_gt={start_date}&game_date_lt={end_date}"

    response = requests.get(full_url, headers=headers, timeout=120)
    response.raise_for_status()

    # Read the data into a DataFrame
    df = pd.read_csv(StringIO(response.text))

    # Save to CSV
    df.to_csv(file_name, index=False)
    print(f"Data saved to {file_name}") 




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
 
        fetch_savant_data(args.start_date, args.end_date, BASE_MLB_URL, HEADERS, "statcast_mlb.csv")

    elif args.league == "milb":

         fetch_savant_data(args.start_date, args.end_date, BASE_MiLB_URL, HEADERS, "statcast_mlb.csv")

    elif args.league == "both":
        fetch_savant_data(args.start_date, args.end_date, BASE_MLB_URL, HEADERS, "statcast_mlb.csv")
        fetch_savant_data(args.start_date, args.end_date, BASE_MiLB_URL, HEADERS, "statcast_milb.csv")

if __name__ == "__main__":
    main()
