import pandas as pd
import argparse
from pybaseball import statcast

def fetch_statcast_data(start_date, end_date, file_name="statcast_data.csv"):
    """Fetch Statcast data using pybaseball and save it to a CSV file."""
    df = statcast(start_dt=start_date, end_dt=end_date)
    
    # Save to CSV
    df.to_csv(file_name, index=False)
    print(f"Data saved to {file_name}")

def main():
    """Main function to handle command-line arguments."""
    parser = argparse.ArgumentParser(description="Download Statcast data for a given date range.")
    parser.add_argument("start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="statcast_data.csv", help="Output CSV file name (default: statcast_data.csv)")

    args = parser.parse_args()
    fetch_statcast_data(args.start_date, args.end_date, args.output)

if __name__ == "__main__":
    main()
