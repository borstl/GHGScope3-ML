"""
Collection of functions to help clean dataframes
"""
import pandas as pd
import numpy as np


def merge_series_custom(timeseries1, timeseries2):
    """
    Merge two series.
    Takes two time series objects and returns a new time series object.
    Iterating over each column in the time series objects and comparing the values according as described below.
    """
    merged = pd.Series(index=timeseries1.index, name=timeseries1.name)
    for column in timeseries1.index:
        # TODO: what happens if there are multiple different indexes in a df?
        timeseries1_col = timeseries1[column]
        timeseries2_col = timeseries2[column]

        # if both values are missing
        if pd.isna(timeseries1_col) and pd.isna(timeseries2_col):
            merged[column] = np.nan
        # if both values are same
        elif timeseries1_col == timeseries2_col:
            merged[column] = timeseries1_col
        # if first value is missing take the second one
        elif pd.isna(timeseries1_col):
            merged[column] = timeseries2_col
        # if second value is missing take the first one
        elif pd.isna(timeseries2_col):
            merged[column] = timeseries1_col
        # if both values are different, print it and take the first one
        # TODO: consider using mean of both values if possible?
        else:
            print(f"Index {column}: values differ - {timeseries1_col} (first), {timeseries2_col} (second)")
            merged[column] = timeseries1_col
    return merged


def handle_duplicated_rows(df):
    """Historic data has sometimes duplicated rows"""
    # Remove empty columns first
    without_empty_columns_df = remove_empty_columns(df)
    # Get duplicated Date Index Series
    duplicate_df = without_empty_columns_df[without_empty_columns_df.index.duplicated(keep=False)]
    merged_series = merge_series_custom(duplicate_df.iloc[0], duplicate_df.iloc[1])
    # replace series with merged series
    transposed_df_series = pd.DataFrame(merged_series).transpose()
    cleaned_df = pd.concat([without_empty_columns_df, transposed_df_series])
    ordered_df = cleaned_df.sort_index()
    return ordered_df


def remove_empty_columns(df):
    """Remove empty columns"""
    return df.dropna(how='all', axis=1, inplace=False)


def aggregate_years(df):
    """Historic data have their row id as date, we want them as a clear year"""
    df_datetime = df
    df_datetime.index = pd.to_datetime(df.index)
    # TODO: use something different else other than 'sum'
    aggregated_df = df_datetime.resample('YE').sum()
    aggregated_df.index = aggregated_df.index.to_period('Y')
    return aggregated_df


def fill_range_of_years(since, till, df):
    """This is to have the same number of rows throughout every dataframe"""
    period = pd.period_range(start=since, end=till, freq='Y')
    df_structure = pd.DataFrame(index=period)
    merged_df = df_structure.join(df, how='left')
    return merged_df


def cleaning_df(df):
    """Coupling the different functions into one function"""
    unique_df = handle_duplicated_rows(df)
    striped_df = aggregate_years(unique_df)
    clean_df = fill_range_of_years(2010, 2024, striped_df)
    return clean_df


if __name__ == "__main__":
    dataframe = pd.read_csv("../data/datasets/DataFrame-time-series-AAPL.OQ.csv", index_col="Date")
    modeled_dataframe = cleaning_df(dataframe)
