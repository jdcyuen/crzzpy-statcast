# writers/csv_writer.py
from src.writers.base_writer import DataWriter
import csv
from tqdm import tqdm
import pandas as pd
import os
import logging

class CSVWriter(DataWriter):

    def __init__(self, output_dir="csv_data"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        logging.debug(f"CSVWriter output directory set to: {self.output_dir}")

    def open(self, file_name: str, append=False):
        """Open CSV file for writing/appending, return (file_handle, csv_writer, header_needed)."""
        file_path = os.path.join(self.output_dir, file_name)
        mode = 'a' if append and os.path.exists(file_path) else 'w'
        f = open(file_path, mode=mode, newline='', encoding='utf-8')
        writer = csv.writer(f)
        header_needed = (mode == 'w')
        return f, writer, header_needed    

    def write(self, csv_writer, df: pd.DataFrame, header_needed: bool = False):
        """Write DataFrame rows to CSV file."""
        if header_needed:
            csv_writer.writerow(df.columns)
        for row in tqdm(df.itertuples(index=False, name=None),
                        total=len(df), desc="Saving to CSV", unit="row"):
            csv_writer.writerow(row)
