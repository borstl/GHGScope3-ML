"""
Collection of functions to help clean dataframes
"""
from datetime import datetime

import pandas as pd
import numpy as np
from pathlib import Path
from pandas import Series
from pandas.core.groupby import DataFrameGroupBy

SINCE: datetime = datetime(2010, 1, 1)
TILL: datetime = datetime(2024, 12, 31)


def merge_duplicates(timeseries1: Series, timeseries2: Series) -> pd.DataFrame:
    """
    Merge two series.
    Takes two time series objects and returns a new time series object.
    Iterating over each column in the time series objects and comparing
    the values according as described below.
    :arg:
    timeseries1 (pd.Series): First time series object
    timeseries2 (pd.Series): Second time series object
    :return: The merged time series object as dataframe
    """
    merged: pd.DataFrame = pd.DataFrame([timeseries1])
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


def remove_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove empty columns"""
    replaced: pd.DataFrame = df.replace("", np.nan)
    return replaced.dropna(how='all', axis=1, inplace=False)


def handle_duplicated_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Historic data has sometimes duplicated rows"""
    if not df.index.has_duplicates:
        return df
    # Get duplicated Date Index Series
    duplicated_indexes: np.ndarray = df.index.duplicated(keep=False)
    duplicates: pd.DataFrame = df[duplicated_indexes]
    merged: pd.DataFrame = merge_duplicates(duplicates.iloc[0], duplicates.iloc[1])
    cleaned: pd.DataFrame = pd.concat([df, merged])
    return cleaned.sort_index()


def resize_to_range_of_years(df: pd.DataFrame) -> pd.DataFrame:
    """This is to have the same range of rows throughout every dataframe"""
    result: pd.DataFrame = df.copy()
    result.resample('1Y', origin=SINCE)
    return result


def aggregate_years(df: pd.DataFrame) -> pd.DataFrame:
    """Historical data have their row id as date, we want them as a clear year"""
    df.index = pd.to_datetime(df.index)
    if df.empty:
        return df[df.index.to_series().between(SINCE, TILL)]
    return (
        df[df.index.to_series().between(SINCE, TILL)]
        .resample('YE')
        .agg(lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA)
    )


def attach_multiindex(df: pd.DataFrame, instrument: str) -> pd.DataFrame:
    """Attach instrument identifier and DateTime Year as a MultiIndex to the dataframe"""
    result: pd.DataFrame = df.copy()
    result.index = pd.MultiIndex.from_product(
        [[instrument], df.index], names=["Instrument", "Date"]
    )
    return result


def standardize_historic(
        instrument: str,
        df: pd.DataFrame
) -> pd.DataFrame:
    """
    Process and standardize historical data.
    Steps:
    1. Handle duplicated rows
    2. Aggregate values by year
    3. Resize dataframe to the range of years
    4. Reindex dataframe to MultiIndex [(Instrument, Date)]
    :arg:
        df (pd.DataFrame): Historical data
        instrument (str): Company Identifier
    :return: standardized historical data
    """
    without_empty_columns: pd.DataFrame = remove_empty_columns(df)
    unique: pd.DataFrame = handle_duplicated_rows(without_empty_columns)
    aggregated: pd.DataFrame = aggregate_years(unique)
    resized: pd.DataFrame = resize_to_range_of_years(aggregated)
    return attach_multiindex(resized, instrument)


def standardize_historic_collection(collection: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Standardize all historical dataframes in a collection"""
    for company in collection:
        collection[company] = standardize_historic(company, collection[company])
    return collection


#TODO can't handle only single company dataframes yet, cause they are not loaded as MultiIndex
def extract_historic_companies(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Group the dataframe by instruments
    WORKAROUND: Only load multiple companies at once
    """
    companies = df.columns.get_level_values(0).unique()
    company_dataframes: dict[str, pd.DataFrame] = {}
    for company in companies:
        df_company: pd.DataFrame = pd.DataFrame(df.xs(key=company, axis=1, level=0))
        company_dataframes.update({company: df_company})
    return company_dataframes


def standardize_static(df: pd.DataFrame) -> pd.DataFrame:
    """Process and standardize static data"""
    aggregated: pd.DataFrame = aggregate_static(df)
    unique: pd.DataFrame = remove_empty_columns(aggregated)
    return unique.set_index('Instrument')


def standardize_static_collection(collection: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Standardize all static dataframes in a collection"""
    for company in collection:
        collection[company] = standardize_static(collection[company])
    return collection


def extract_static_companies(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Group the dataframe by instruments"""
    grouped: DataFrameGroupBy = df.groupby('Instrument')
    company_dataframes: dict[str, pd.DataFrame] = {}
    for instrument in grouped.groups:
        company_dataframes.update({str(instrument): grouped.get_group(instrument)})
    return company_dataframes


def aggregate_static(df: pd.DataFrame) -> pd.DataFrame:
    """Split all static rows by instruments"""
    return pd.DataFrame(
        df
        .groupby("Instrument")
        .agg(
            lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA
        )
        .reset_index()
    )


def remove_all_same_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove all columns where all values in that column are the same.
    This is to reduce the dimensionality of the dataframe.
    :arg: df (pd.DataFrame): Dataframe to be cleaned
    :return: Dataframe with columns where no column has all the same values
    """
    return df.loc[:, (df != df.iloc[0]).any()]


def historical_medians(df: pd.DataFrame) -> list[str]:
    """
    Spot sudden step changes in reported columns. If the values in the column are far
    larger or smaller than its historical median, then this is likely to be the result
    of either a change in calculation methodology or the result of some large corporate
    change. E.g., merger and acquisitions activity.
    :arg: df (pd.DataFrame): Historical data
    :return: List of columns with sudden step changes
    """
    abrupt_change_columns: list[str] = []
    for column in df.columns:
        if df[column].dtype in ('float', 'int'):
            median: float = df[column].median(numeric_only=True)
            if df[column].max() > median * 2:
                abrupt_change_columns.append(column)
            if df[column].min() < median / 2:
                abrupt_change_columns.append(column)
    return abrupt_change_columns


def read_all_csv(directory: Path) -> dict[str, pd.DataFrame]:
    """Read all csv files in a given directory"""
    data_dict: dict[str, pd.DataFrame] = {}
    for file in directory.glob("*.csv"):
        if file.is_file():
            data_dict.update({file.name: pd.read_csv(file)})
    return data_dict


def combine_all_static_frames(data_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine all static dataframes into one"""
    return pd.concat(data_dict.values()).reset_index(drop=True)


def combine_all_historic_frames(data_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine all historic dataframes into one"""
    return pd.concat(data_dict.values())



