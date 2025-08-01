"""
Collection of functions to help clean dataframes
"""
import datetime
from datetime import datetime

import pandas as pd
import numpy as np
from pandas import Series, DataFrame

EXAMPLE_CSV_PATH: str = "../data/datasets/Example/CompanyA/DataFrame-Historic-Example-Company-A-First-Half.csv"
SINCE: datetime = datetime(2010, 1, 1)
TILL: datetime = datetime(2024, 12, 31)


def merge_duplicates(timeseries1: Series, timeseries2: Series) -> DataFrame:
    """
    Merge two series.
    Takes two time series objects and returns a new time series object.
    Iterating over each column in the time series objects and comparing
    the values according as described below.
    """
    merged: DataFrame = pd.DataFrame([timeseries1])
    for column in timeseries1.index:
        # TODO: what happens if there are multiple different duplicated indexes in a df?
        # if both values are missing
        if pd.isna(timeseries1[column]) and pd.isna(timeseries2[column]):
            merged[column] = np.nan
        # if first value is missing take the second one
        elif pd.isna(timeseries1[column]):
            merged[column] = timeseries2[column]
        # if second value is missing take the first one
        elif pd.isna(timeseries2[column]):
            merged[column] = timeseries1[column]
        # if both values are same
        elif timeseries1[column] == timeseries2[column]:
            merged[column] = timeseries1[column]
        # if both values are different, print it and take the first one
        # TODO: consider using mean of both values if possible?
        else:
            print(f"Index {column}: values differ - {timeseries1[column]} (first), {timeseries2[column]} (second)")
            merged[column] = timeseries1[column]
    return merged.convert_dtypes()


def handle_duplicated_rows(df: DataFrame) -> DataFrame:
    """Historic data has sometimes duplicated rows"""
    if not df.index.has_duplicates:
        return df

    # Remove empty columns first
    without_empty_columns: DataFrame = remove_empty_columns(df)
    # Get duplicated Date Index Series
    duplicated_indexes: np.ndarray = without_empty_columns.index.duplicated(keep=False)
    duplicates: DataFrame = without_empty_columns[duplicated_indexes]
    merged: DataFrame = merge_duplicates(duplicates.iloc[0], duplicates.iloc[1])
    cleaned: DataFrame = pd.concat([without_empty_columns, merged])
    return cleaned.sort_index()


def remove_empty_columns(dataframe: DataFrame) -> DataFrame:
    """Remove empty columns"""
    replaced: DataFrame = dataframe.replace("", np.nan)
    return replaced.dropna(how='all', axis=1, inplace=False)


def aggregate_years(dataframe: DataFrame) -> DataFrame:
    """Historic data have their row id as date, we want them as a clear year"""
    dataframe.index = pd.to_datetime(dataframe.index)
    aggregated: DataFrame = (
        dataframe[dataframe.index.to_series().between(SINCE, TILL)]
        .resample('YE')
        .agg(lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA)
    )
    return aggregated.to_period('Y')


def fill_range_of_years(df: DataFrame) -> DataFrame:
    """This is to have the same number of rows throughout every dataframe"""
    structure: DataFrame = pd.DataFrame(
        index=pd.period_range(start=SINCE, end=TILL, freq='Y')
    )
    # TODO
    debug = structure.join(df)
    return debug

def cleaning_history(df: DataFrame) -> DataFrame:
    """Coupling the different functions into one function"""
    unique: DataFrame = handle_duplicated_rows(df)
    striped_dataframe: DataFrame = aggregate_years(unique)
    clean: DataFrame = fill_range_of_years(striped_dataframe)
    clean.index.name = "Date"
    return clean


def aggregate_static(df: DataFrame) -> DataFrame:
    """Aggregate all static rows"""
    df = remove_empty_columns(df)
    return df.agg(
        lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA
    ).to_frame().transpose()


def group_static(df: DataFrame) -> DataFrame:
    """Split all static rows by instruments"""
    print("Grouping static rows by instruments")
    grouped: DataFrame = (
        df
        .groupby("Instrument")
        .agg(
            lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA
        )
        .reset_index()
    )
    return remove_empty_columns(grouped)


if __name__ == "__main__":
    data = pd.read_csv(EXAMPLE_CSV_PATH, index_col="Date")
    modeled_dataframe = cleaning_history(data)
