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
from pandas import DataFrame
from functions.cleaning import cleaning_history, group_static
from functions.modeling import join_static_and_historic, concat_companies
from functions.parameters import Parameter

warnings.simplefilter(action='ignore', category=FutureWarning)


def parallel_download(threads: int):
    """Downloading frames from LSEG database concurrently"""
    with ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(download_all_frames, params.companies)


def download_all_frames(companies: list[str]):
    """Downloading all frames from LSEG database"""
    companies_chunks: list[list[str]] = split_in_chunks(companies, 10)
    # with ThreadPoolExecutor(max_workers=10) as executor:
    #    static_result = executor.map(download_all_static_chunks, companies_chunks)
    #with ThreadPoolExecutor(max_workers=1) as executor:
    #    historic_result = executor.map(download_all_historic_chunks, companies_chunks)
    download_all_historic_chunks(companies_chunks[0])
    # company_dataframe: DataFrame = join_static_and_historic(static, historic)
    # company_dataframe.to_csv(parameters.SAFE_DATA_PATH + "frame.csv")
    print("done")


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


def download_all_static_chunks(companies: list[str]) -> DataFrame:
    """Downloading all static fields from a list of companies"""
    chunks: list[list] = split_in_chunks(
        params.static_fields,
        parameters.CHUNK_SIZE,
        parameters.CHUNK_LIMIT
    )
    dataframe: DataFrame = pd.DataFrame()

    for chunk in progressbar.progressbar(chunks, prefix="Downloading static data " + companies[0]):
        #try:
            new_data: DataFrame = ld.get_data(
                universe=companies,
                fields=chunk,
                header_type=HeaderType.NAME,
            )
            clean_dataframe = group_static(new_data)
            if dataframe.empty:
                dataframe = clean_dataframe
            else:
                dataframe = dataframe.merge(clean_dataframe, how="left")
        #except Exception as e:
        #    print(e)
    dataframe.to_csv("../data/datasets/static/companies-from-" + companies[0] + ".csv", index=False)
    return dataframe


def download_all_historic_chunks(companies: list[str]) -> DataFrame:
    """Downloading all fields from a company and join them together"""
    chunks: list[list] = split_in_chunks(
        params.historic_fields,
        parameters.CHUNK_SIZE,
        parameters.CHUNK_LIMIT
    )
    skipped_list: list[list] = chunks[8:]
    dataframe: DataFrame = pd.DataFrame()
    for chunk in progressbar.progressbar(skipped_list, prefix="Downloading history data " + companies[0]):
        #try:
            new_data: DataFrame = download_historic_from(companies, chunk)
            clean_dataframe = cleaning_history(new_data)
            if dataframe.empty:
                dataframe = clean_dataframe
            else:
                dataframe = dataframe.join(clean_dataframe, validate='one_to_one')
        #except Exception as e:
        #    print(e)
    dataframe.to_csv("../data/datasets/historic/companies-from-" + companies[0] + ".csv", index=False)
    return dataframe


def download_historic_from(companies: list[str], fields: list[str]) -> DataFrame:
    """Downloading content of with time series fields from a company"""
    return ld.get_history(
        universe=companies,
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
    ld.open_session()
    # parallel_download(1)
    download_all_frames(params.companies)
    ld.close_session()
