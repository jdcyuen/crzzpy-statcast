# writers/csv_writer.py
from writers.base_writer import DataWriter
from google.cloud import bigquery
import pandas as pd
from tqdm import tqdm

class BQWriter(DataWriter):
    def write(self, df: pd.DataFrame, table_name: str, league: str, chunk_size: int):
        client = bigquery.Client()

        # Load job config to overwrite the table
        bq_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        current_year = datetime.now().year
        table_name = f"statcast_{current_year}_{league}"
        table_id = f"crzzpy.test.{table_name}"

       # If you want to truncate first, do it for the first chunk
        first = True

        # Progress bar over chunks
        for i in tqdm(range(0, len(df), chunk_size), desc="Uploading to BigQuery"):
            chunk = df.iloc[i:i+chunk_size]

            # Optionally truncate on first chunk
            if first:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                first = False
            else:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            job = client.load_table_from_dataframe(chunk, table_id, job_config=job_config)
            job.result()