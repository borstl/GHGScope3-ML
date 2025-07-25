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
PARAMS: dict = {
    "SDate": "CY2010",
    "EDate": "CY2024",
    "Period": "FY0",
    "Frq": "CY" # Yearly frequency
}
CHUNK_SIZE: int = 10
CHUNK_LIMIT: int = 3

warnings.simplefilter(action='ignore', category=FutureWarning)


def download_content(companies_path, static_fields_path, time_series_fields_path):
    """Downloading content from LSEG database"""
    with open(companies_path, encoding="utf-8") as f:
        companies: list[str] = [line.strip() for line in f]
    with open(static_fields_path, encoding="utf-8") as f:
        static_fields: list[str] = [line.strip() for line in f]
    with open(time_series_fields_path, encoding="utf-8") as f:
        time_series_fields: list[str] = [line.strip() for line in f]

    full_dataframe: DataFrame = pd.DataFrame()
    ld.open_session()
    for company in companies:
        print("Downloading static of " + company)
        static_dataframe = download_all_static_chunks(company, static_fields)

        print("Downloading time series of " + company)
        historic_dataframe = download_all_time_series_chunks(company, time_series_fields)

        company_frame = join_static_and_historic(static_dataframe, historic_dataframe)

        if full_dataframe.empty:
            date_frame = pd.DataFrame(company_frame.index.to_series(), columns=['Date'])
            company_frame.insert(0, 'Date', date_frame)
            full_dataframe = company_frame
        else:
            debug_dataframe = concat_companies(full_dataframe, company_frame)
            full_dataframe = debug_dataframe

        # reduced_df = remove_empty_columns(full_df)
        # full_dataframe.to_csv("../data/datasets/" + company + ".csv", index_label="Date")
    ld.close_session()


def split_in_chunks(field_list: list[str],
                    chunk_size:int = CHUNK_SIZE,
                    chunk_limit: int | None = None) -> list[list]:
    """Splitting a list in chunks of 1000 items"""
    chunks: list[list] = []
    for i in range(0, len(field_list), chunk_size):
        chunks.append(field_list[i: i + chunk_size])
    if chunk_limit is not None:
        return chunks[:chunk_limit]
    return chunks


def download_all_static_chunks(company: str, fields: list[str]) -> DataFrame:
    """Downloading all static fields from a company"""
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


if __name__ == "__main__":
    download_content(
        "../data/parameter/company.txt", STATIC_FIELDS_PATH, TIME_SERIES_FIELDS_PATH
    )
