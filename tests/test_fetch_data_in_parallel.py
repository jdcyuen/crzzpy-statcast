import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import pandas as pd
import datetime
import builtins
import io
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Patch tqdm globally in this test file to disable progress bars
import statcast_fetch
statcast_fetch.tqdm = lambda *args, **kwargs: args[0]  # 👈 disables tqdm in fetch logic

from statcast_fetch import _fetch_data_in_parallel

class DummyTqdm:
    def __init__(self, iterable=None, **kwargs):
        self.iterable = iterable
    def __iter__(self): return iter(self.iterable or [])
    def update(self, *args): pass
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass

# Override progress bar in tests
statcast_fetch.TQDM_WRAPPER = DummyTqdm

class TestFetchDataInParallel(unittest.TestCase):

    @patch("statcast_fetch._fetch_chunk")
    @patch("statcast_fetch.open", new_callable=mock_open)
    def test_parallel_fetch_and_write(self, mock_file, mock_fetch_chunk):
        # Setup dummy DataFrame to return
        dummy_df = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        mock_fetch_chunk.return_value = dummy_df

        file_name = "dummy_output.csv"

        _fetch_data_in_parallel(
            start_date="2024-04-01",
            end_date="2024-04-03",
            base_url="http://fake-url.com",
            headers={},
            parameters={"type": "details"},
            file_name=file_name,
            chunk_size=2,
            step_days=None,
            max_workers=2,
            writer=None  # use default writer
        )

        mock_file.assert_called_once_with(file_name, mode='w', newline='', encoding='utf-8')
        self.assertGreaterEqual(mock_fetch_chunk.call_count, 1)

    @patch("statcast_fetch._fetch_chunk")
    @patch("statcast_fetch.open", new_callable=mock_open)
    def test_no_data_fetched(self, mock_file, mock_fetch_chunk):
        mock_fetch_chunk.return_value = pd.DataFrame()  # Empty DataFrame

        _fetch_data_in_parallel(
            start_date="2024-04-01",
            end_date="2024-04-03",
            base_url="http://fake-url.com",
            headers={},
            parameters={"type": "details"},
            file_name="dummy_output.csv",
            chunk_size=2,
            step_days=None,
            max_workers=2,
            writer=None
        )

        mock_file.assert_not_called()
        self.assertGreaterEqual(mock_fetch_chunk.call_count, 1)

    @patch("statcast_fetch._fetch_chunk")
    @patch("statcast_fetch.open", new_callable=mock_open)
    def test_mixed_data_chunks(self, mock_file, mock_fetch_chunk):
        df_valid = pd.DataFrame({"col1": [1], "col2": ["X"]})
        mock_fetch_chunk.side_effect = [df_valid, pd.DataFrame(), None]

        _fetch_data_in_parallel(
            start_date="2024-04-01",
            end_date="2024-04-05",
            base_url="http://fake-url.com",
            headers={},
            parameters={"type": "details"},
            file_name="mixed_output.csv",
            chunk_size=2,
            step_days=None,
            max_workers=2,
            writer=None
        )

        mock_file.assert_called_once()
        self.assertEqual(mock_fetch_chunk.call_count, 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
