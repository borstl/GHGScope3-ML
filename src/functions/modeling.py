"""
Functions to get the data into the right format to prepare for training
"""
import pandas as pd


def append_company_to(dataframe):
    """
    The dataframe of a new company should be appended to the other ones
    | company_id | year | revenue |
    | A | 2020 | 4$ |
    | A | 2021 | 3$ |
    | B | 2020 | 3$ |
    | B | 2021 | 5$ |
    """
    return dataframe.append(dataframe)


def join_static_and_historic(static_dataframe, historic_dataframe):
    """Merge static and historic data into one dataframe"""
    blown_up_static_dataframe = blow_up(static_dataframe, 2010, 2024)
    merged_dataframe = historic_dataframe.join(blown_up_static_dataframe, how='left', validate='one_to_one')
    return merged_dataframe


def blow_up(dataframe, since, till):
    """Duplicate rows in static dataframe until it has the sice of historic dataframes (e.g. row 2010-2024"""
    for _ in range(since, till):
        dataframe = pd.concat([dataframe, dataframe.iloc[[0]]], ignore_index=True)
    period = pd.period_range(start=since, end=till, freq='Y')
    dataframe.set_index(period, inplace=True)
    dataframe.index.name = "Date"
    return dataframe


def concat_companies(dataframe, new_data):
    """Merge new data into one dataframe"""
    dataframe = pd.concat([dataframe, new_data], ignore_index=True, sort=False)
    return dataframe


if __name__ == "__main__":
    static_df = pd.read_csv("../data/datasets/Example/CompanyA/DataFrame-Static-Example-Company-A-First-Half.csv",
                            index_col="Date")
    historic_df = pd.read_csv("../data/datasets/Example/CompanyA/DataFrame-Historic-Example-Company-A-First-Half.csv",
                              index_col="Date")
    join_static_and_historic(static_df, historic_df)
