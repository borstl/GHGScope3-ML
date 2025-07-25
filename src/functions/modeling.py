"""
Functions to get the data into the right format to prepare for training
"""
import datetime

import pandas as pd
from pandas import DataFrame, PeriodIndex

CSV_COMPANY_1_PATH: str = "../data/datasets/Example/CompanyA/DataFrame-Static-Example-Company-A-First-Half.csv"
CSV_COMPANY_2_PATH: str = "../data/datasets/Example/CompanyA/DataFrame-Historic-Example-Company-A-First-Half.csv"
SINCE: datetime = datetime.date(2010, 1, 1)
TILL: datetime = datetime.date(2024, 12, 31)


def join_static_and_historic(static: DataFrame, historic: DataFrame) -> DataFrame:
    """Merge static and historic data into one dataframe"""
    blown_up_static: DataFrame = blow_up(static, SINCE, TILL)
    return historic.join(blown_up_static, how='left', validate='one_to_one')


def blow_up(df: DataFrame, since: datetime, till: datetime) -> DataFrame:
    """
    Duplicate rows in static dataframe until it has the sice of
    historic dataframes (e.g. row 2010-2024)
    :rtype: DataFrame
    """
    for _ in range(int(since.year), int(till.year)):
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    period: PeriodIndex = pd.period_range(start=since, end=till, freq='Y')
    df.set_index(period, inplace=True)
    df.index.name = "Date"
    return df


def concat_companies(df: DataFrame, new_data: DataFrame):
    """Merge new data into one dataframe"""
    dates: DataFrame = pd.DataFrame(new_data.index.to_series(), columns=['Date'])
    new_data.insert(0, 'Date', dates)
    df = pd.concat([df, new_data], ignore_index=True, sort=False)
    return df


if __name__ == "__main__":
    static_df: DataFrame = (pd.read_csv(CSV_COMPANY_1_PATH, index_col="Date"))
    historic_df: DataFrame = pd.read_csv(CSV_COMPANY_2_PATH, index_col="Date")
    join_static_and_historic(static_df, historic_df)
