import unittest
from unittest.mock import patch, Mock
import datetime
import pandas as pd
import io
import sys
import os

# Make sure we can import from ../src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from statcast_fetch import _fetch_chunk


class TestFetchChunk(unittest.TestCase):

    @patch("statcast_fetch.requests.get")
    def test_fetch_chunk_success(self, mock_get):
        # Simulate CSV response content
        csv_data = "col1,col2\n1,A\n2,B"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = csv_data.encode("utf-8")
        mock_get.return_value = mock_response

        df = _fetch_chunk(
            "2024-04-01", "2024-04-02",
            base_url="http://fake-url.com",
            headers={},
            parameters={"type": "details"}
        )

        expected_df = pd.read_csv(io.StringIO(csv_data))
        pd.testing.assert_frame_equal(df, expected_df)

    @patch("statcast_fetch.requests.get")
    def test_fetch_chunk_http_error(self, mock_get):
        # Simulate HTTP error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")
        mock_get.return_value = mock_response

        df = _fetch_chunk(
            "2024-04-01", "2024-04-02",
            base_url="http://fake-url.com",
            headers={},
            parameters={"type": "details"},
            max_retries=1
        )

        self.assertIsNone(df)

    @patch("statcast_fetch.requests.get")
    def test_fetch_chunk_empty_data(self, mock_get):
        # Simulate empty CSV response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_get.return_value = mock_response

        df = _fetch_chunk(
            "2024-04-01", "2024-04-02",
            base_url="http://fake-url.com",
            headers={},
            parameters={"type": "details"}
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)


if __name__ == "__main__":
    unittest.main(verbosity=2)
