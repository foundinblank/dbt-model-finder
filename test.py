# %%
import unittest
import pandas as pd
from app import clean_input as clean_app_input
from pandas.testing import assert_frame_equal

class TestApp(unittest.TestCase):
    def test_dbt_cloud_run1(self):
        """
        Test test_cases/dbt_cloud_run1.txt
        """
        with open("test_cases/dbt_cloud_run1.txt") as f:
            test_input = f.read()
        
        self.assertTrue(
            assert_frame_equal(
                pd.read_csv("test_cases/solutions/dbt_cloud_run1.csv"),
                clean_app_input(test_input),
            )
            is None
        )

    def test_dbt_core_run1(self):
        """
        Test test_cases/dbt_core_run1.txt
        """
        with open("test_cases/dbt_core_run1.txt") as f:
            test_input = f.read()
        
        self.assertTrue(
            assert_frame_equal(
                pd.read_csv("test_cases/solutions/dbt_core_run1.csv"),
                clean_app_input(test_input),
            )
            is None
        )

    def test_dbt_core_run2(self):
        """
        Test test_cases/dbt_core_run2.txt
        """
        with open("test_cases/dbt_core_run2.txt") as f:
            test_input = f.read()
        
        self.assertTrue(
            assert_frame_equal(
                pd.read_csv("test_cases/solutions/dbt_core_run2.csv"),
                clean_app_input(test_input),
            )
            is None
        )

    def test_dbt_core_source_freshness1(self):
        """
        Test test_cases/dbt_core_source_freshness1.txt
        """
        with open("test_cases/dbt_core_source_freshness1.txt") as f:
            test_input = f.read()

        self.assertTrue(
            assert_frame_equal(
                pd.read_csv("test_cases/solutions/dbt_core_source_freshness1.csv"),
                clean_app_input(test_input),
            )
            is None
        )


if __name__ == "__main__":
    unittest.main()
