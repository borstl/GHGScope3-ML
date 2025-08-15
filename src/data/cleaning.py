"""
Collection of functions to help clean dataframes
"""
from datetime import datetime
import pandas as pd
import numpy as np
from pandas import Series, DataFrame, PeriodIndex
from pandas.core.groupby import DataFrameGroupBy

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


def remove_empty_columns(df: DataFrame) -> DataFrame:
    """Remove empty columns"""
    replaced: DataFrame = df.replace("", np.nan)
    return replaced.dropna(how='all', axis=1, inplace=False)


def aggregate_years(df: DataFrame) -> DataFrame:
    """Historical data have their row id as date, we want them as a clear year"""
    df.index = pd.to_datetime(df.index)
    return (
        df[df.index.to_series().between(SINCE, TILL)]
        .resample('YE')
        .agg(lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA)
    )


def fill_range_of_years(df: DataFrame) -> DataFrame:
    """This is to have the same number of rows throughout every dataframe"""
    structure: DataFrame = pd.DataFrame(
        index=pd.period_range(start=SINCE, end=TILL, freq='Y', name='Date'),
    )
    return df.reindex(index=structure.index)


def cleaning_history(df: DataFrame) -> DataFrame:
    """Coupling the different functions into one function"""
    unique: DataFrame = handle_duplicated_rows(df)
    striped: DataFrame = aggregate_years(unique)
    #TODO: rewrite to support multiple companies in one dataframe
    filled: DataFrame = fill_range_of_years(striped)
    if isinstance(filled.columns, pd.MultiIndex):
        filled.columns = filled.columns.droplevel(0)
    return filled


def aggregate_static(df: DataFrame) -> DataFrame:
    """Split all static rows by instruments"""
    return DataFrame(
        df
        .groupby("Instrument")
        .agg(
            lambda col: col.dropna().iloc[0] if col.notna().any() else pd.NA
        )
        .reset_index()
    )


def clean_static(df: DataFrame) -> DataFrame:
    """clean static rows"""
    grouped: DataFrame = aggregate_static(df)
    return remove_empty_columns(grouped)


def join_static_and_historic(static: DataFrame, historic: DataFrame) -> DataFrame:
    """Merge static and historic data into one dataframe for one company"""
    blown_up_static: DataFrame = blow_up(static)
    return historic.join(blown_up_static, how='left', validate='one_to_one')


def join_all(static: DataFrame, historic: DataFrame) -> DataFrame:
    """Join static data and historic data into one dataframe for all companies"""
    stretched: DataFrame = stretch_static(static)
    return historic.join(stretched, how='left', validate='one_to_one')


def duplicate_group(df: DataFrame, times: int) -> DataFrame:
    """Duplicate a group of rows"""
    return pd.concat([df] * times, ignore_index=True)


def stretch_static(df: DataFrame) -> DataFrame:
    """All statistical data for each company should be duplicated to the size
     of the historic data to prepare a one-to-one merge"""
    return pd.DataFrame(
        df.groupby('Instrument', group_keys=False).apply(
            lambda g: duplicate_group(g, (TILL.year - SINCE.year))
        )
        .reset_index(drop=True)
    )


def blow_up(df: DataFrame) -> DataFrame:
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


def concat_companies(df: DataFrame, new_data: DataFrame) -> DataFrame:
    """Merge new data into one dataframe"""
    dates: DataFrame = pd.DataFrame(new_data.index.to_series(), columns=['Date'])
    new_data.insert(0, 'Date', dates)
    df = pd.concat([df, new_data], ignore_index=True, sort=False)
    return df


def join(static: DataFrame, timeseries: DataFrame) -> DataFrame:
    """Join static and timeseries dataframes"""
    grouped_static: DataFrameGroupBy = static.groupby('Instrument')
    grouped_timeseries: DataFrameGroupBy = timeseries.groupby('Instrument')
    return DataFrame(
        grouped_static.apply(
            lambda g: grouped_timeseries.get_group([g.name]).join(g, on='Insturment', how='left')
        )
    )
