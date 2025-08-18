"""
Collection of functions to help clean dataframes
"""
from datetime import datetime
from logging import Logger

import pandas as pd
import numpy as np
from pandas import Series, PeriodIndex
from pandas.core.groupby import DataFrameGroupBy

SINCE: datetime = datetime(2010, 1, 1)
TILL: datetime = datetime(2024, 12, 31)


def merge_duplicates(timeseries1: Series, timeseries2: Series) -> pd.DataFrame:
    """
    Merge two series.
    Takes two time series objects and returns a new time series object.
    Iterating over each column in the time series objects and comparing
    the values according as described below.
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

    # Remove empty columns first
    without_empty_columns: pd.DataFrame = remove_empty_columns(df)
    # Get duplicated Date Index Series
    duplicated_indexes: np.ndarray = without_empty_columns.index.duplicated(keep=False)
    duplicates: pd.DataFrame = without_empty_columns[duplicated_indexes]
    merged: pd.DataFrame = merge_duplicates(duplicates.iloc[0], duplicates.iloc[1])
    cleaned: pd.DataFrame = pd.concat([without_empty_columns, merged])
    return cleaned.sort_index()


def resize_to_range_of_years(df: pd.DataFrame) -> pd.DataFrame:
    """This is to have the same range of rows throughout every dataframe"""
    result: pd.DataFrame = df.copy()
    result.resample('1Y', origin=SINCE)
    return result


def aggregate_years(df: pd.DataFrame) -> pd.DataFrame:
    """Historical data have their row id as date, we want them as a clear year"""
    df.index = pd.to_datetime(df.index)
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


def standardize_instrument_history(
        df: pd.DataFrame,
        instrument: str,
) -> pd.DataFrame:
    """
    Process and clean historical data.
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
    unique: pd.DataFrame = handle_duplicated_rows(df)
    aggregated: pd.DataFrame = aggregate_years(unique)
    resized: pd.DataFrame = resize_to_range_of_years(aggregated)
    return attach_multiindex(resized, instrument)


def extract_companies(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Group the dataframe by instruments"""
    companies = df.columns.get_level_values(0).unique()
    company_dataframes: dict[str, pd.DataFrame] = {}

    for company in companies:
        df_company: pd.DataFrame = pd.DataFrame(df.xs(key=company, axis=1, level=0))
        company_dataframes[company] = df_company
    return company_dataframes


def resize_to_range(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing years to model the data to a certain range"""
    return pd.DataFrame(
        df.groupby('Instrument').apply(lambda g: resize_to_range_of_years(g))
        .reset_index(drop=True)
    )


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


def clean_static(df: pd.DataFrame) -> pd.DataFrame:
    """clean static rows"""
    grouped: pd.DataFrame = aggregate_static(df)
    return remove_empty_columns(grouped)


def join_static_and_historic(static: pd.DataFrame, historic: pd.DataFrame) -> pd.DataFrame:
    """Merge static and historic data into one dataframe for one company"""
    blown_up_static: pd.DataFrame = blow_up(static)
    return historic.join(blown_up_static, how='left', validate='one_to_one')


def join_all(static: pd.DataFrame, historic: pd.DataFrame) -> pd.DataFrame:
    """Join static data and historic data into one dataframe for all companies"""
    stretched: pd.DataFrame = stretch_static(static)
    return historic.join(stretched, how='left', validate='one_to_one')


def duplicate_group(df: pd.DataFrame, times: int) -> pd.DataFrame:
    """Duplicate a group of rows"""
    return pd.concat([df] * times, ignore_index=True)


def stretch_static(df: pd.DataFrame) -> pd.DataFrame:
    """All statistical data for each company should be duplicated to the size
     of the historic data to prepare a one-to-one merge"""
    return pd.DataFrame(
        df.groupby('Instrument', group_keys=False).apply(
            lambda g: duplicate_group(g, (TILL.year - SINCE.year))
        )
        .reset_index(drop=True)
    )


def blow_up(df: pd.DataFrame) -> pd.DataFrame:
    """
    Duplicate rows in static dataframe until it has the sice of
    historic dataframes (e.g. row 2010-2024)
    """
    for _ in range(int(SINCE.year), int(TILL.year)):
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    period: PeriodIndex = pd.period_range(start=SINCE, end=TILL, freq='Y')
    df.set_index(period, inplace=True)
    df.index.name = "Date"
    return df


def concat_companies(df: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """Merge new data into one dataframe"""
    dates: pd.DataFrame = pd.DataFrame(new_data.index.to_series(), columns=['Date'])
    new_data.insert(0, 'Date', dates)
    df = pd.concat([df, new_data], ignore_index=True, sort=False)
    return df


def join(static: pd.DataFrame, timeseries: pd.DataFrame) -> pd.DataFrame:
    """Join static and timeseries dataframes"""
    grouped_static: DataFrameGroupBy = static.groupby('Instrument')
    grouped_timeseries: DataFrameGroupBy = timeseries.groupby('Instrument')
    return pd.DataFrame(
        grouped_static.apply(
            lambda g: grouped_timeseries.get_group([g.name]).join(g, on='Insturment', how='left')
        )
    )
