import numpy as np
import pandas as pd
from typing import Iterator, Tuple, Literal


def calculate_median(dataframe: pd.DataFrame,
                     grouping_by: str = 'TR.GICSSectorCode',
                     target: str = 'TR.UpstreamScope3PurchasedGoodsAndServices') -> pd.DataFrame:
    fine_grouping: list[str] = ['Date', grouping_by]
    coarse_grouping: list[str] = [grouping_by]

    df: pd.DataFrame = dataframe.copy()
    column_names: pd.Index = df.columns
    num_cols: pd.Index = df.select_dtypes(include=['Float64', 'Int64']).columns
    int_cols: pd.Index = df.select_dtypes(include='Int64').columns
    # exclude Scope 3.1 because it is the target variable
    num_cols = num_cols.drop(target)

    df[int_cols] = df[int_cols].astype('Float64')

    sector_fine_grp = df.groupby(fine_grouping, observed=True)[num_cols]
    sector_coarse_grp = df.groupby(coarse_grouping, observed=True)[num_cols]

    sector_medians_fine: pd.DataFrame = sector_fine_grp.median()
    sector_medians_coarse: pd.DataFrame = sector_coarse_grp.median()
    medians_combined: pd.DataFrame = sector_medians_fine.where(
        ~sector_medians_fine.isna(), sector_medians_coarse
    )

    df = df.set_index(fine_grouping)
    mask = df[num_cols].isna()
    df[num_cols] = df[num_cols].where(~mask, medians_combined)
    df[num_cols] = df[num_cols].fillna(df[num_cols].median())
    df[int_cols] = df[int_cols].round().astype('Int64')
    df = df.reset_index(drop=False)
    df = df.reindex(columns=column_names)

    return df


def _first_mode(df: pd.DataFrame):
    m = df.mode()
    return m.iloc[0] if not m.empty else np.nan


# Mode for columns with categorical types
def calculate_mode(dataframe: pd.DataFrame, grouping_by: str = 'TR.GICSSectorCode') -> pd.DataFrame:
    fine_grouping: list[str] = ['Date', grouping_by]
    coarse_grouping: list[str] = [grouping_by]

    df: pd.DataFrame = dataframe.copy()
    column_names: pd.Index = df.columns
    cat_cols: pd.Index = df.select_dtypes(include=['object', 'category', 'string', 'bool']).columns
    cat_cols = cat_cols.drop(['Instrument', 'Date', grouping_by])

    modes_fine = (
        df.groupby(fine_grouping, observed=True)[cat_cols]
        .agg(_first_mode)
    )
    modes_coarse = (
        df.groupby(coarse_grouping, observed=True)[cat_cols]
        .agg(_first_mode)
    )
    modes_combined = modes_fine.where(~modes_fine.isna(), modes_coarse)

    df = df.set_index(fine_grouping)
    mask = df[cat_cols].isna()
    df[cat_cols] = df[cat_cols].where(~mask, modes_combined)
    df = df.reset_index(drop=False)
    df = df.reindex(columns=column_names)

    return df


def fill_na_by_modes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Example how one loop of fill_na_by_modes looks like:

    modes: dict[str, str] = {}
    for group in filtered_df.groupby("TR.HQCountryCode")['Currency Code']:
        country = group[0]
        currency = group[1].mode().iloc[0]
        modes[country] = currency
    mask = filtered_df['Currency Code'].isna()
    filtered_df.loc[mask, 'Currency Code'] = filtered_df.loc[mask, 'TR.HQCountryCode'].map(modes)

    :param df: pd.DataFrame
    :return: pd.DataFrame
    """

    fill_key_by_modes_of_value: dict[str, str] = {
        'Currency Code': 'TR.HQCountryCode',
        'TR.AssetCategory': 'TR.GICSSectorCode',
        'TR.BusinessSector': 'TR.GICSSectorCode',
        'TR.BusinessSectorScheme': 'TR.GICSSectorCode',
        'TR.CompanyParentType': 'TR.GICSSectorCode',
        'TR.HeadquartersRegionAlt': 'TR.HQCountryCode',
        'TR.InstrumentType': 'TR.GICSSectorCode',
        'TR.OrganizationType': 'TR.GICSSectorCode',
        'TR.PriceMainIndex': 'TR.HQCountryCode',
        'TR.RelatedOrgISO2': 'TR.HQCountryCode',
        'TR.RelatedOrgType': 'TR.GICSSectorCode',
    }
    df = df.copy()
    modes: dict[str, str]
    mask: pd.Series

    for missing_value_col, col in fill_key_by_modes_of_value.items():
        modes = {}
        groups: Iterator[Tuple[str, pd.Series]] = iter(
            df.groupby(col)[missing_value_col]
        )

        for group in groups:
            modes[group[0]] = group[1].mode().iloc[0]

        mask = df[missing_value_col].isna()
        df.loc[mask, missing_value_col] = df.loc[mask, col].map(modes)

    return df


def fill_na_by_median(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    medians: dict[str, int] = {}
    groups: Iterator[Tuple[str, pd.Series]] = iter(
        df.groupby('TR.GICSSectorCode')['Total Share Float']
    )

    for group in groups:
        total_shares = group[1].dropna().round()
        medians[group[0]] = int(total_shares.median())

    mask = df['Total Share Float'].isna()
    df.loc[mask, 'Total Share Float'] = df.loc[mask, 'TR.GICSSectorCode'].map(medians)

    return df
