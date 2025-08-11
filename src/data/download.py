"""
LSEG Data Download Module

This module handles downloading financial and ESG data from the LSEG database.
It includes retry logic, chunking for large datasets, and error handling.
"""
import logging
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import lseg.data as ld
from lseg.data import HeaderType
from pandas import DataFrame

from core import Config
from core.exceptions import DataDownloadError
from .cleaning import cleaning_history, concat_companies, group_static

warnings.simplefilter(action='ignore', category=FutureWarning)


def split_in_chunks(
        field_list: list[str],
        chunk_size: int,
        chunk_limit: int = 0,
        skipped_chunks: int = 0
) -> list[list]:
    """Splitting a list in chunks of e.g. 1000 items"""
    chunks: list[list] = []
    for i in range(0, len(field_list), chunk_size):
        chunks.append(field_list[i: i + chunk_size])
    if chunk_limit > 0 and skipped_chunks > 0:
        return chunks[skipped_chunks:chunk_limit]
    if chunk_limit > 0 >= skipped_chunks:
        return chunks[:chunk_limit]
    if chunk_limit <= 0 < skipped_chunks:
        return chunks[skipped_chunks:]
    return chunks


def get_empty_columns_names(csv):
    """Returns all empty column names from a data frame as a list"""
    df = pd.read_csv(csv)
    na_df = df.replace("", pd.NA)
    return na_df.columns[df.isna().all()].tolist()


def bundle(starter: DataFrame, incoming: DataFrame) -> DataFrame:
    """Concatenate two company dataframes if there is already one filled with data"""
    if starter.empty:
        date_frame = pd.DataFrame(incoming.index.to_series(), columns=['Date'])
        incoming.insert(0, 'Date', date_frame)
        return incoming
    return concat_companies(starter, incoming)


class LSEGDataDownloader:
    """Downloading Data from LSEG API"""
    config: Config = None
    logger: logging.Logger = None
    session_open: bool = False

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.session_open = False

    def __enter__(self) -> "LSEGDataDownloader":
        self.open_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_session()

    def open_session(self) -> None:
        """Open session for LSEG API"""
        ld.open_session()
        self.session_open = True

    def close_session(self) -> None:
        """Close session for LSEG API"""
        ld.close_session()
        self.session_open = False

    def download_all_frames(self) -> None:
        """Downloading all frames from LSEG database"""
        companies_chunks: list[list[str]] = split_in_chunks(
            self.config.companies,
            self.config.companies_chunk_size
        )
        with ThreadPoolExecutor(self.config.max_workers) as executor:
            executor.map(self.download_all_static_chunks, companies_chunks)
        with ThreadPoolExecutor(self.config.max_workers) as executor:
            executor.map(self.download_all_historic_chunks, companies_chunks)

    def download_all_static_chunks(self, companies: list[str]) -> DataFrame:
        """Downloading all static fields from a list of companies"""
        chunks: list[list] = split_in_chunks(
            self.config.static_features,
            self.config.chunk_size_static
        )
        df: DataFrame = self.download_static_from(companies, chunks[0])
        df = group_static(df)
        company_index: int = 1
        for chunk in chunks[1:]:
            try:
                new_data: DataFrame = self.download_static_from(companies, chunk)
                clean_df: DataFrame = group_static(new_data)
                df = df.merge(clean_df, how="left")
                company_index += 1
            except DataDownloadError:
                company_index += 1
        df.to_csv(
            self.config.data_dir / "datasets" / "static" / f"companies-{companies[0]}.csv",
            index=False
        )
        return df

    def download_static_from(self, companies: list[str], chunk: list[str]) -> DataFrame:
        """Downloading static fields from a list of companies"""
        tries: int = 10
        delay: int = 1
        backoff: int = 2

        for _ in range(tries):
            try:
                return ld.get_data(universe=companies, fields=chunk, header_type=HeaderType.NAME)
            except DataDownloadError:
                time.sleep(delay)
                delay *= backoff
        self.logger.info(
            "Couldn't download static data for companies: %s and chunks: %s",
            companies, chunk
        )
        raise ConnectionError(f"Connection failed for {companies} companies")

    def download_all_historic_chunks(self, companies: list[str]) -> DataFrame:
        """Downloading all fields from a company and join them together"""
        chunks: list[list] = split_in_chunks(
            self.config.historic_features,
            self.config.chunk_size_historic
        )
        df: DataFrame = self.download_historic_from(companies, chunks[0])
        df = cleaning_history(df)
        company_index: int = 1
        for chunk in chunks[1:]:
            try:
                new_data: DataFrame = self.download_historic_from(companies, chunk)
                clean_dataframe = cleaning_history(new_data)
                df = df.join(clean_dataframe, validate='one_to_one')
                company_index += 1
            except DataDownloadError:
                company_index += 1
        df.to_csv(
            self.config.data_dir / "datasets" / "historic" / f"companies-{companies[0]}.csv",
            index=False
        )
        return df

    def download_historic_from(self, companies: list[str], chunk: list[str]) -> DataFrame:
        """Downloading content of with time series fields from a company"""
        for n in range(self.config.max_retries):
            try:
                return ld.get_history(
                    universe=companies,
                    fields=chunk,
                    parameters=self.config.params,
                    header_type=HeaderType.NAME,
                )
            except DataDownloadError as e:
                time.sleep(self.config.retry_delay)
                self.config.retry_delay *= self.config.retry_backoff_multiplier
                print(f"Connection {n}/{self.config.max_retries} failed."
                      f"Retrying in {self.config.retry_delay} seconds. {e}"
                      )
        self.logger.info(
            "Couldn't download historic data for companies: %s and chunks: %s",
            companies, chunk
        )
        raise ConnectionError(f"Connection failed for {companies} companies")

    def download_gics_codes(self):
        """Downloading the GICS sector codes of all companies"""
        gics_codes: DataFrame = ld.get_data(
            universe=self.config.companies,
            fields=["TR.GICSIndustryCode", "TR.GICSIndustry"],
        )
        gics_codes.to_csv(self.config.data_dir / "datasets" / "gics_industry_codes.csv")
