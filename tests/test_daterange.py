import unittest
import datetime
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from statcast_fetch import _daterange  # Replace with your actual module name if different

class TestDateRange(unittest.TestCase):

    def test_basic_chunking(self):
        start_date = datetime.date(2024, 4, 1)
        end_date = datetime.date(2024, 4, 10)
        chunk_size = 3  # Expect chunks of 3 days

        expected_chunks = [
            (1, datetime.date(2024, 4, 1), datetime.date(2024, 4, 3)),
            (2, datetime.date(2024, 4, 4), datetime.date(2024, 4, 6)),
            (3, datetime.date(2024, 4, 7), datetime.date(2024, 4, 9)),
            (4, datetime.date(2024, 4, 10), datetime.date(2024, 4, 10))
        ]

        result = list(_daterange(start_date, end_date, chunk_size))
        print("🔍 Result from _daterange:", result)  # 👈 Add this line

        self.assertEqual(result, expected_chunks)


    def test_with_step_days(self):
        start_date = datetime.date(2024, 4, 1)
        end_date = datetime.date(2024, 4, 10)
        chunk_size = 3
        step_days = 2  # Overlapping chunks

        expected_chunks = [
            (1, datetime.date(2024, 4, 1), datetime.date(2024, 4, 3)),
            (2, datetime.date(2024, 4, 3), datetime.date(2024, 4, 5)),
            (3, datetime.date(2024, 4, 5), datetime.date(2024, 4, 7)),
            (4, datetime.date(2024, 4, 7), datetime.date(2024, 4, 9)),
            (5, datetime.date(2024, 4, 9), datetime.date(2024, 4, 10))
        ]

        result = list(_daterange(start_date, end_date, chunk_size, step_days))
        self.assertEqual(result, expected_chunks)

    def test_same_start_end_date(self):
        """Test when start_date == end_date"""
        start = end = datetime.date(2024, 4, 1)
        result = list(_daterange(start, end, chunk_size=3))
        expected = [(1, start, end)]
        self.assertEqual(result, expected)

    def test_start_after_end(self):
        """Test when start_date > end_date"""
        start = datetime.date(2024, 4, 5)
        end = datetime.date(2024, 4, 1)
        result = list(_daterange(start, end, chunk_size=3))
        self.assertEqual(result, [])  # No chunks expected

    def test_chunk_size_larger_than_range(self):
        """Test when chunk_size > total days"""
        start = datetime.date(2024, 4, 1)
        end = datetime.date(2024, 4, 3)
        result = list(_daterange(start, end, chunk_size=10))
        expected = [(1, start, end)]
        self.assertEqual(result, expected)

    def test_step_days_greater_than_chunk_size(self):
        """Test when step_days > chunk_size"""
        start = datetime.date(2024, 4, 1)
        end = datetime.date(2024, 4, 10)
        result = list(_daterange(start, end, chunk_size=2, step_days=4))
        expected = [
            (1, datetime.date(2024, 4, 1), datetime.date(2024, 4, 2)),
            (2, datetime.date(2024, 4, 5), datetime.date(2024, 4, 6)),
            (3, datetime.date(2024, 4, 9), datetime.date(2024, 4, 10))
            ]
        self.assertEqual(result, expected)
    

if __name__ == "__main__":
    unittest.main(verbosity=2)
