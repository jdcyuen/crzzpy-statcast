# writers/base_writer.py
from abc import ABC, abstractmethod
import pandas as pd

class DataWriter(ABC):
    @abstractmethod
    def write(self, df: pd.DataFrame, file_name: str):
        pass