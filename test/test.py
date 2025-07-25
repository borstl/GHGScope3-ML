"""
Test the functions with example data.
"""
import unittest
import pandas as pd
from functions.cleaning import cleaning_history


class TestFunctions(unittest.TestCase):
    """Test the functions with example data."""

    def test_join_historic_df(self):
        example_df = pd.read_csv("../src/Data/Datasets/Example/DataFrame-Historic-Example-Company-A-Full.csv",
                                 index_col="Date")
        first_df = pd.read_csv(
            "../src/Data/Datasets/Example/CompanyA/DataFrame-Historic-Example-Company-A-First-Half.csv",
            index_col="Date"
        )
        second_df = pd.read_csv(
            "../src/Data/Datasets/Example/CompanyA/DataFrame-Historic-Example-Company-A-Second-Half.csv",
            index_col="Date"
        )
        clean_first_df = cleaning_history(first_df)
        clean_second_df = cleaning_history(second_df)
        joined_df = clean_first_df.join(clean_second_df, validate='one_to_one')
        joined_df.index.name = "Date"
        example_df.index = pd.to_datetime(example_df.index, format="%Y").to_period('Y')
        self.assertEqual(joined_df, example_df)


if __name__ == "__main__":
    unittest.main()
