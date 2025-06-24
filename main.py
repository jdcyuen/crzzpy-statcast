# main.py
from flask import Request
from src.statcast_fetch import run_statcast_pipeline

def run_statcast(request: Request):
    """HTTP Cloud Function entry point."""
    request_json = request.get_json(silent=True)
    request_args = request.args

    start_date = request_json.get("start_date") if request_json else None
    end_date = request_json.get("end_date") if request_json else None
    league = request_json.get("league", "mlb") if request_json else "mlb"
    file_name = request_json.get("file", f"statcast_{league}.csv")
    
    # Basic validation
    if not start_date or not end_date:
        return "Missing required parameters: start_date and end_date", 400

    run_statcast_pipeline(
        start_date=start_date,
        end_date=end_date,
        league=league,
        file_name=file_name,
        progress=False,  # disable tqdm for GCF
        log_level="INFO"
    )

    return f"✅ Statcast data for {league} from {start_date} to {end_date} fetched successfully."
