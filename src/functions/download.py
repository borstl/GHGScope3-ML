"""
functions to download content from the LSEG database
"""
import warnings
import lseg.data as ld
import pandas as pd
import progressbar

from lseg.data import HeaderType
from pandas import DataFrame
from functions.cleaning import cleaning_history, aggregate_static
from functions.modeling import join_static_and_historic, concat_companies

COMPANIES_PATH: str = "../data/parameter/companies.txt"
STATIC_FIELDS_PATH: str = "../data/parameter/tr_values_static.txt"
TIME_SERIES_FIELDS_PATH: str = "../data/parameter/tr_values_history.txt"
# SKIP: int = 22_800
PARAMS: dict = {
    "SDate": "CY2010",
    "EDate": "CY2024",
    "Period": "FY0",
    "Frq": "CY"  # Yearly frequency
}
CHUNK_SIZE: int = 100
CHUNK_LIMIT: int | None = None

warnings.simplefilter(action='ignore', category=FutureWarning)


def download_content(companies_path, static_fields_path, time_series_fields_path):
    """Downloading content from LSEG database"""
    with open(companies_path, encoding="utf-8") as f:
        companies: list[str] = [line.strip() for line in f]
    with open(static_fields_path, encoding="utf-8") as f:
        static_fields: list[str] = [line.strip() for line in f]
    with open(time_series_fields_path, encoding="utf-8") as f:
        historic_fields: list[str] = [line.strip() for line in f]
    download_all_frames(companies, static_fields, historic_fields)


def download_all_frames(companies, static_fields, historic_fields):
    """Downloading all frames from LSEG database"""
    full_dataframe: DataFrame = pd.DataFrame()
    ld.open_session()
    for company in companies:
        static: DataFrame = download_all_static_chunks(company, static_fields)
        # included_fields: list[str] = static_fields[SKIP:]
        # static_dataframe = download_all_static_chunks(company, included_fields)
        # static_dataframe.to_csv("../data/datasets/static/" + company + ".csv")
        historic: DataFrame = download_all_time_series_chunks(company, historic_fields)
        company_dataframe: DataFrame = join_static_and_historic(static, historic)
        full_dataframe = bundle(full_dataframe, company_dataframe)
    ld.close_session()


def bundle(new_dataframe: DataFrame, old_dataframe: DataFrame) -> DataFrame:
    """Concatenate two company dataframes if there is already one filled with data"""
    if old_dataframe.empty:
        date_frame = pd.DataFrame(new_dataframe.index.to_series(), columns=['Date'])
        new_dataframe.insert(0, 'Date', date_frame)
        return new_dataframe
    return concat_companies(old_dataframe, new_dataframe)


def split_in_chunks(
        field_list: list[str],
        chunk_size: int = CHUNK_SIZE,
        chunk_limit: int | None = None
) -> list[list]:
    """Splitting a list in chunks of e.g. 1000 items"""
    chunks: list[list] = []
    for i in range(0, len(field_list), chunk_size):
        chunks.append(field_list[i: i + chunk_size])
    if chunk_limit is not None:
        return chunks[:chunk_limit]
    return chunks


def download_all_static_chunks(company: str, fields: list[str]) -> DataFrame:
    """Downloading all static fields from a company"""
    print("Downloading static of " + company)
    chunks: list[list] = split_in_chunks(fields, CHUNK_SIZE, CHUNK_LIMIT)
    dataframe: DataFrame = pd.DataFrame()
    for chunk in progressbar.progressbar(chunks):
        new_data: DataFrame = ld.get_data(
            universe=company,
            fields=chunk,
            header_type=HeaderType.NAME,
        )
        clean_dataframe = aggregate_static(new_data)
        if dataframe.empty:
            dataframe = clean_dataframe
        else:
            dataframe = dataframe.merge(clean_dataframe, how="left")
    return dataframe


def download_all_time_series_chunks(company: str, fields: list[str]) -> DataFrame:
    """Downloading all fields from a company and join them together"""
    print("Downloading time series of " + company)
    chunks: list[list] = split_in_chunks(fields, CHUNK_SIZE, CHUNK_LIMIT)
    dataframe: DataFrame = pd.DataFrame()
    for chunk in progressbar.progressbar(chunks):
        new_data: DataFrame = download_time_series_from(company, chunk)
        clean_dataframe = cleaning_history(new_data)
        if dataframe.empty:
            dataframe = clean_dataframe
        else:
            dataframe = dataframe.join(clean_dataframe, validate='one_to_one')
    return dataframe


def download_time_series_from(company: str, fields: list[str]) -> DataFrame:
    """Downloading content of with time series fields from a company"""
    return ld.get_history(
        universe=company,
        fields=fields,
        # interval="yearly",
        # start="2010-01-01",
        # end="2024-12-31",
        parameters=PARAMS,
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
    download_content(
        "../data/parameter/company.txt", STATIC_FIELDS_PATH, TIME_SERIES_FIELDS_PATH
    )
