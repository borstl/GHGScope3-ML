"""
Collection of functions to help clean dataframes
"""
import datetime
from datetime import datetime

import pandas as pd
import numpy as np
from pandas import Series, DataFrame

EXAMPLE_CSV_PATH: str = "../data/datasets/Example/CompanyA/DataFrame-Historic-Example-Company-A-First-Half.csv"
SINCE: datetime = datetime(2020, 1, 1)
TILL: datetime = datetime(2024, 12, 31)


def merge_duplicates(timeseries1: Series, timeseries2: Series) -> DataFrame:
    """
    Merge two series.
    Takes two time series objects and returns a new time series object.
    Iterating over each column in the time series objects and comparing
    the values according as described below.
    """
    merged: Series = pd.Series(index=timeseries1.index, name=timeseries1.name)
    for column in timeseries1.index:
        # TODO: what happens if there are multiple different duplicated indexes in a df?
        timeseries1_col: Series = timeseries1[column]
        timeseries2_col: Series = timeseries2[column]

        # if both values are missing
        if pd.isna(timeseries1_col) and pd.isna(timeseries2_col):
            merged[column] = np.nan
        # if first value is missing take the second one
        elif pd.isna(timeseries1_col):
            merged[column] = timeseries2_col
        # if second value is missing take the first one
        elif pd.isna(timeseries2_col):
            merged[column] = timeseries1_col
        # if both values are same
        elif timeseries1_col == timeseries2_col:
            merged[column] = timeseries1_col
        # if both values are different, print it and take the first one
        # TODO: consider using mean of both values if possible?
        else:
            print(f"Index {column}: values differ - {timeseries1_col} (first), {timeseries2_col} (second)")
            merged[column] = timeseries1_col
    return merged.to_frame().transpose()


def handle_duplicated_rows(df: DataFrame) -> DataFrame:
    """Historic data has sometimes duplicated rows"""
    if df.index.has_duplicates:
        # Remove empty columns first
        # TODO: maybe not remove emtpy columns before concat them all
        without_empty_columns: DataFrame = remove_empty_columns(df)
        # Get duplicated Date Index Series
        duplicated_indexes: np.ndarray = without_empty_columns.index.duplicated(keep=False)
        duplicates: DataFrame = without_empty_columns[duplicated_indexes]
        merged: DataFrame = merge_duplicates(duplicates.iloc[0], duplicates.iloc[1])
        cleaned: DataFrame = pd.concat([without_empty_columns, merged])
        ordered: DataFrame = cleaned.sort_index()
        return ordered
    return df


def remove_empty_columns(dataframe: DataFrame) -> DataFrame:
    """Remove empty columns"""
    replaced: DataFrame = dataframe.replace("", np.nan)
    return replaced.dropna(how='all', axis=1, inplace=False)


def aggregate_years(df: DataFrame) -> DataFrame:
    """Historic data have their row id as date, we want them as a clear year"""
    df.index = pd.to_datetime(df.index)
    # TODO: use other aggregation function than 'sum'
    # TODO: drop empty fields per column and then aggregate real values
    aggregated: DataFrame = df.resample('YE').apply(custom_resampler)
    aggregated.index = aggregated.to_period('Y')
    return aggregated


def custom_resampler(arraylike: Series):
    """Something"""
    # TODO: some series have different types as values e.g. int and string
    non_null: Series = arraylike.dropna()
    result = non_null
    return result


def fill_range_of_years(since: datetime, till: datetime, dataframe: DataFrame) -> DataFrame:
    """This is to have the same number of rows throughout every dataframe"""
    period = pd.period_range(start=since, end=till, freq='Y')
    dataframe_structure = pd.DataFrame(index=period)
    merged_dataframe = dataframe_structure.join(dataframe, how='left')
    return merged_dataframe


def cleaning_history(dataframe: DataFrame) -> DataFrame:
    """Coupling the different functions into one function"""
    unique_dataframe = handle_duplicated_rows(dataframe)
    striped_dataframe = aggregate_years(unique_dataframe)
    clean_dataframe = fill_range_of_years(SINCE, TILL, striped_dataframe)
    clean_dataframe.index.name = "Date"
    return clean_dataframe


def aggregate_static(df: DataFrame) -> DataFrame:
    """Aggregate all static rows"""
    df = remove_empty_columns(df)
    return df.agg(
        lambda col: col.dropna().iloc[0]if col.notna().any() else pd.NA
    ).to_frame().transpose()


if __name__ == "__main__":
    data = pd.read_csv(EXAMPLE_CSV_PATH, index_col="Date")
    modeled_dataframe = cleaning_history(data)
