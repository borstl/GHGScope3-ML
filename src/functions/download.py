"""
functions to download content from the LSEG database
"""
import warnings
import lseg.data as ld
import pandas as pd
import progressbar
import parameters

from concurrent.futures import ThreadPoolExecutor
from lseg.data import HeaderType
from lseg.data._errors import LDError
from pandas import DataFrame
from functions.cleaning import cleaning_history, aggregate_static
from functions.modeling import join_static_and_historic, concat_companies
from functions.parameters import Parameter

warnings.simplefilter(action='ignore', category=FutureWarning)


def parallel_download(threads: int):
    """Downloading frames from LSEG database concurrently"""
    ld.open_session()
    with ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(download_all_frames, params.companies)
    ld.close_session()


def download_all_frames(company):
    """Downloading all frames from LSEG database"""
    static: DataFrame = download_all_static_chunks(company)
    historic: DataFrame = download_all_historic_chunks(company)
    company_dataframe: DataFrame = join_static_and_historic(static, historic)
    company_dataframe.to_csv(parameters.SAFE_DATA_PATH + company + ".csv")


def bundle(incoming: DataFrame, starter: DataFrame) -> DataFrame:
    """Concatenate two company dataframes if there is already one filled with data"""
    if starter.empty:
        date_frame = pd.DataFrame(incoming.index.to_series(), columns=['Date'])
        incoming.insert(0, 'Date', date_frame)
        return incoming
    return concat_companies(starter, incoming)


def split_in_chunks(
        field_list: list[str],
        chunk_size: int = parameters.CHUNK_SIZE,
        chunk_limit: int | None = None
) -> list[list]:
    """Splitting a list in chunks of e.g. 1000 items"""
    chunks: list[list] = []
    for i in range(0, len(field_list), chunk_size):
        chunks.append(field_list[i: i + chunk_size])
    if chunk_limit is not None:
        return chunks[:chunk_limit]
    return chunks


def download_all_static_chunks(company: str) -> DataFrame:
    """Downloading all static fields from a company"""
    chunks: list[list] = split_in_chunks(
        params.static_fields,
        parameters.CHUNK_SIZE,
        parameters.CHUNK_LIMIT
    )
    dataframe: DataFrame = pd.DataFrame()

    try:
        for chunk in progressbar.progressbar(chunks, prefix="Downloading static data of " + company):
            new_data: DataFrame = ld.get_data(
                universe=params.companies,
                fields=chunk,
                header_type=HeaderType.NAME,
            )
            clean_dataframe = aggregate_static(new_data)
            if dataframe.empty:
                dataframe = clean_dataframe
            else:
                dataframe = dataframe.merge(clean_dataframe, how="left")
    except LDError as e:
            print("Static data couldn't be loaded for company " + company + ". " + e.message)
    return dataframe


def download_all_historic_chunks(company: str) -> DataFrame:
    """Downloading all fields from a company and join them together"""
    chunks: list[list] = split_in_chunks(
        params.historic_fields,
        parameters.CHUNK_SIZE,
        parameters.CHUNK_LIMIT
    )
    dataframe: DataFrame = pd.DataFrame()
    try:
        for chunk in progressbar.progressbar(chunks, prefix="Downloading history of " + company):
            new_data: DataFrame = download_historic_from(company, chunk)
            clean_dataframe = cleaning_history(new_data)
            if dataframe.empty:
                dataframe = clean_dataframe
            else:
                dataframe = dataframe.join(clean_dataframe, validate='one_to_one')
    except LDError as e:
            print("Historic data couldn't be loaded for company " + company + ". " + e.message)
    return dataframe


def download_historic_from(company: str, fields: list[str]) -> DataFrame:
    """Downloading content of with time series fields from a company"""
    return ld.get_history(
        universe=company,
        fields=fields,
        # interval="yearly",
        # start="2010-01-01",
        # end="2024-12-31",
        parameters=parameters.PARAMS,
        header_type=HeaderType.NAME,
    )


def get_empty_columns_names(csv):
    """Returns all empty column names from a data frame as a list"""
    df = pd.read_csv(csv)
    na_df = df.replace("", pd.NA)
    return na_df.columns[df.isna().all()].tolist()


def download_gics_codes():
    """Downloading the GICS sector codes of all companies"""
    companies = []
    with open("../data/parameter/companies.txt", encoding="utf-8") as f:
        for line in f:
            companies.append(line.strip())
    ld.open_session()
    gics_codes = ld.get_data(
        universe=companies,
        fields=["TR.GICSIndustryCode", "TR.GICSIndustry"],
    )
    ld.close_session()
    gics_codes.to_csv("../data/datasets/gics_industry_codes.csv")


def configure_lseg():
    """Configure LSEG"""
    config = ld.get_config()
    config.set_param("logs.transports.console.enabled", True)
    config.set_param("logs.level", "debug")
    config.set_param("logs.transports.file.name", "lseg-data-lib.log")
    config.set_param("http.request-timeout", 600_000)


if __name__ == "__main__":
    params = Parameter()
    parallel_download(1)
