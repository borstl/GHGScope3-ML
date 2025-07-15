"""
Functions to get the data into the right format to prepare for training
"""
from cleaning import cleaning, fill_range_of_years
import pandas as pd

def append_company_to_dataframe(df):
    """
    The dataframe of a new company should be appended to the other ones
    | company_id | year | revenue |
    | A | 2020 | 4$ |
    | A | 2021 | 3$ |
    | B | 2020 | 3$ |
    | B | 2021 | 5$ |
    """
    return df.append(df)


def merge_static_and_historic(static_df, historic_df):
    blown_up_df = blow_up_rows(static_df)
    merged_df = historic_df.merge(blown_up_df, how='inner', validate='one_to_one')
    return merged_df


def blow_up_rows(df):
    blown_up_df = fill_range_of_years(df)
    return blown_up_df


if __name__ == "__main__":
    static_df = pd.read_csv("../data/datasets/Example/CompanyA/DataFrame-Static-Example-Company-A-First-Half.csv", index_col="Date")
    historic_df = pd.read_csv("../data/datasets/Example/CompanyA/DataFrame-Historic-Example-Company-A-First-Half.csv", index_col="Date")
    merge_static_and_historic(static_df, historic_df)