"""
functions to download content from the LSEG database
"""
import time
import warnings
import lseg.data as ld
import pandas as pd
import progressbar as pb
from lseg.data._errors import LDError
import parameters
import logging

from concurrent.futures import ThreadPoolExecutor
from lseg.data import HeaderType
from pandas import DataFrame
from functions.cleaning import cleaning_history, group_static
from functions.modeling import join_static_and_historic, concat_companies
from functions.parameters import Parameter

warnings.simplefilter(action='ignore', category=FutureWarning)

logger = logging.getLogger(__name__)


def parallel_download(threads: int):
    """Downloading frames from LSEG database concurrently"""
    with ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(download_all_frames, params.companies)


def download_all_frames(companies: list[str]):
    """Downloading all frames from LSEG database"""
    companies_chunks: list[list[str]] = split_in_chunks(companies, 10, skipped_chunks=None)
    with ThreadPoolExecutor(max_workers=10) as executor:
        static_result = executor.map(download_all_static_chunks, companies_chunks)
    with ThreadPoolExecutor(max_workers=10) as executor:
        historic_result = executor.map(download_all_historic_chunks, companies_chunks)
    #company_dataframe: DataFrame = join_static_and_historic(static_result, historic_result)
    #company_dataframe.to_csv(parameters.SAFE_DATA_PATH + "frame.csv")
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
        chunk_limit: int | None = parameters.CHUNK_LIMIT,
        skipped_chunks: int | None = parameters.SKIP_CHUNKS,
) -> list[list]:
    """Splitting a list in chunks of e.g. 1000 items"""
    chunks: list[list] = []
    for i in range(0, len(field_list), chunk_size):
        chunks.append(field_list[i: i + chunk_size])
    if chunk_limit is not None and skipped_chunks is not None:
        return chunks[skipped_chunks:chunk_limit]
    if chunk_limit is not None and skipped_chunks is None:
        return chunks[:chunk_limit]
    if chunk_limit is None and skipped_chunks is not None:
        return chunks[skipped_chunks:]
    return chunks


def download_all_static_chunks(companies: list[str]) -> DataFrame:
    """Downloading all static fields from a list of companies"""
    chunks: list[list] = split_in_chunks(params.static_fields)
    dataframe: DataFrame = download_static_from(companies, chunks[0])
    company_index: int = 1
    for chunk in pb.progressbar(chunks[1:], prefix="Downloading static data " + companies[company_index]):
        new_data: DataFrame = download_static_from(companies, chunk)
        clean_dataframe = group_static(new_data)
        dataframe = dataframe.merge(clean_dataframe, how="left")
        company_index += 1
    dataframe.to_csv("../data/datasets/static/companies-from-" + companies[0] + ".csv", index=False)
    return dataframe


def download_static_from(companies: list[str], chunk: list[str]) -> DataFrame:
    """Downloading static fields from a list of companies"""
    return ld.get_data(universe=companies, fields=chunk, header_type=HeaderType.NAME)


def download_all_historic_chunks(companies: list[str]) -> DataFrame:
    """Downloading all fields from a company and join them together"""
    chunks: list[list] = split_in_chunks(params.historic_fields)
    df: DataFrame = download_historic_from(companies, chunks[0])
    df = cleaning_history(df)
    company_index: int = 1
    for chunk in pb.progressbar(chunks[1:], prefix="Downloading history data " + companies[company_index]):
        new_data: DataFrame = download_historic_from(companies, chunk)
        clean_dataframe = cleaning_history(new_data)
        df = df.join(clean_dataframe, validate='one_to_one')
        company_index += 1
    df.to_csv(
        f"../data/datasets/historic/companies-from-{companies[0]}.csv",
        index=False
    )
    return df


def download_historic_from(companies: list[str], fields: list[str]) -> DataFrame:
    """Downloading content of with time series fields from a company"""
    tries: int = 10
    delay: int = 1
    backoff: int = 2

    for n in range(tries):
        try:
            return ld.get_history(
                universe=companies,
                fields=fields,
                parameters=parameters.PARAMS,
                header_type=HeaderType.NAME,
            )
        except LDError as e:
            time.sleep(delay)
            delay *= backoff
            print(f"Connection {n}/{tries} failed. Retrying in {delay} seconds. {e}")
    raise ConnectionError(f"Connection failed for {companies} companies")


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
    config.set_param("http.request-timeout", 60_000)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    params = Parameter()
    ld.open_session()
    # parallel_download(1)
    download_all_frames(params.companies)
    ld.close_session()
