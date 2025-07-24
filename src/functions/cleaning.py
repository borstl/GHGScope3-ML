"""
Collection of functions to help clean dataframes
"""
import pandas as pd
import numpy as np
from pandas import Series


def merge_series_custom(timeseries1, timeseries2):
    """
    Merge two series.
    Takes two time series objects and returns a new time series object.
    Iterating over each column in the time series objects and comparing the values according as described below.
    """
    merged = pd.Series(index=timeseries1.index, name=timeseries1.name)
    for column in timeseries1.index:
        # TODO: what happens if there are multiple different duplicated indexes in a df?
        timeseries1_col = timeseries1[column]
        timeseries2_col = timeseries2[column]

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
    return merged


def handle_duplicated_rows(dataframe):
    """Historic data has sometimes duplicated rows"""
    if dataframe.index.has_duplicates:
        # Remove empty columns first
        # TODO: maybe not remove emtpy columns before concat them all
        without_empty_columns_dataframe = remove_empty_columns(dataframe)
        # Get duplicated Date Index Series
        duplicate_index = without_empty_columns_dataframe[without_empty_columns_dataframe.index.duplicated(keep=False)]
        merged_series = merge_series_custom(duplicate_index.iloc[0], duplicate_index.iloc[1])
        # replace series with merged series
        transposed_dataframe_series = pd.DataFrame(merged_series).transpose()
        cleaned_dataframe = pd.concat([without_empty_columns_dataframe, transposed_dataframe_series])
        ordered_dataframe = cleaned_dataframe.sort_index()
        return ordered_dataframe
    return dataframe


def remove_empty_columns(dataframe):
    """Remove empty columns"""
    dataframe = dataframe.replace("", np.nan)
    return dataframe.dropna(how='all', axis=1, inplace=False)


def aggregate_years(df):
    """Historic data have their row id as date, we want them as a clear year"""
    df.index = pd.to_datetime(df.index)
    # TODO: use other aggregation function than 'sum'
    # TODO: drop empty fields per column and then aggregate real values
    aggregated_dataframe = df.resample('YE').apply(custom_resampler)
    aggregated_dataframe.index = aggregated_dataframe.index.to_period('Y')
    return aggregated_dataframe


def custom_resampler(arraylike: Series):
    # TODO: some series have different types as values e.g. int and string
    non_null_series = arraylike.dropna()
    result = non_null_series.mean()
    return result


def fill_range_of_years(since, till, dataframe):
    """This is to have the same number of rows throughout every dataframe"""
    period = pd.period_range(start=since, end=till, freq='Y')
    dataframe_structure = pd.DataFrame(index=period)
    merged_dataframe = dataframe_structure.join(dataframe, how='left')
    return merged_dataframe


def cleaning(dataframe):
    """Coupling the different functions into one function"""
    unique_dataframe = handle_duplicated_rows(dataframe)
    striped_dataframe = aggregate_years(unique_dataframe)
    clean_dataframe = fill_range_of_years(2010, 2024, striped_dataframe)
    clean_dataframe.index.name = "Date"
    return clean_dataframe


def aggregate_static(dataframe):
    """Aggregate all static rows"""
    dataframe = remove_empty_columns(dataframe)
    # drop all null per series(columns) and get the first value
    new_dataframe = dataframe.apply(lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA).to_frame().T
    return new_dataframe


if __name__ == "__main__":
    data = pd.read_csv("../data/datasets/Example/CompanyA/DataFrame-Historic-Example-Company-A-First-Half.csv",
                            index_col="Date")
    modeled_dataframe = cleaning(data)
