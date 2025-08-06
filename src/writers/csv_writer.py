# writers/csv_writer.py
from writers.base_writer import DataWriter
import csv
from tqdm import tqdm

class CSVWriter(DataWriter):
    def write(self, df: pd.DataFrame, file_name: str):
        with open(file_name, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(df.columns)
            for row in tqdm(df.itertuples(index=False, name=None), total=len(df), desc="Saving to CSV", unit="row"):
                writer.writerow(row)