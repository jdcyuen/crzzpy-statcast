import pandas as pd
import argparse
from pybaseball import statcast
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_statcast_data(start_date, end_date, file_name="statcast_data.csv"):
    """Fetch Statcast data using pybaseball and save it to a CSV file."""
    df = statcast(start_dt=start_date, end_dt=end_date)
    
    # Save to CSV
    df.to_csv(file_name, index=False)
    print(f"Data saved to {file_name}")

def _fetch_chunk(start_date, end_date, base_url):
    url = f"{base_url}?all=true&&game_date_gt={start_date}&game_date_lt={end_date}"
    try:
        df = pd.read_csv(url)
        print(f"Fetched data from {start_date} to {end_date} ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"Failed to fetch data from {start_date} to {end_date}: {e}")
        return pd.DataFrame()

def fetch_mlb_savant_data(start_date, end_date, file_name="statCast_2025_all.csv"):
    url = f"https://baseballsavant.mlb.com/statcast_search/csv?all=true&&game_date_gt={start_date}&game_date_lt={end_date}"

    # Read the data into a DataFrame
    df = pd.read_csv(url)
    
    # Save to CSV
    df.to_csv(file_name, index=False)
    print(f"Data saved to {file_name}")  

def _daterange(start_date, end_date, delta_days):
    current = start_date
    while current < end_date:
        next_date = min(current + timedelta(days=delta_days), end_date)
        yield current, next_date
        current = next_date 


def main():
    """Main function to handle command-line arguments."""
    parser = argparse.ArgumentParser(description="Download Statcast data for a given date range.")
    parser.add_argument("start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="statcast_data.csv", help="Output CSV file name (default: statcast_data.csv)")

    args = parser.parse_args()
    # fetch_statcast_data(args.start_date, args.end_date, args.output)
    fetch_mlb_savant_data(args.start_date, args.end_date, args.output)

if __name__ == "__main__":
    main()
