"""
LSEG Data Download Module

This module handles downloading financial and ESG data from the LSEG database.
It includes retry logic, chunking for large datasets, and error handling.
"""
import logging
import pathlib
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import lseg.data as ld
from lseg.data import HeaderType
from lseg.data._errors import LDError
from pandas import DataFrame

from core import Config
from core.exceptions import DataDownloadError
from .cleaning import concat_companies, aggregate_static, \
    remove_empty_columns, join

warnings.simplefilter(action='ignore', category=FutureWarning)


def get_empty_columns_names(csv: str | pathlib.Path) -> list[str]:
    """Returns all empty column names from a data frame as a list"""
    df: DataFrame = pd.read_csv(csv)
    na_df: DataFrame = df.replace("", pd.NA)
    return na_df.columns[df.isna().all()].tolist()


def bundle(starter: DataFrame, incoming: DataFrame) -> DataFrame:
    """Concatenate two company dataframes if there is already one filled with data"""
    if starter.empty:
        df = pd.DataFrame(incoming.index.to_series(), columns=['Date'])
        incoming.insert(0, 'Date', df)
        return incoming
    return concat_companies(starter, incoming)


class LSEGDataDownloader:
    """Downloading Data from LSEG API"""
    config: Config = None
    _logger: logging.Logger | None = None
    session_open: bool = False

    def __init__(self, config: Config):
        self.config = config
        self.session_open = False

    @property
    def logger(self) -> logging.Logger:
        """Returns logger"""
        if self._logger is None:
            self._logger = logging.getLogger()
        return self._logger

    @logger.setter
    def logger(self, value) -> None:
        self._logger = value

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
        self.logger.info("Downloading all frames from LSEG database")
        all_static: DataFrame
        all_historic: DataFrame
        with ThreadPoolExecutor(self.config.max_workers) as executor:
            static_results = executor.map(
                self.download_all_static_chunks,
                self.config.companies_chunks
            )
            all_static = pd.concat(static_results, ignore_index=True)
            all_static.to_csv(self.config.data_dir / "datasets" / "static" / "static.csv")
        with ThreadPoolExecutor(self.config.max_workers) as executor:
            historic_results = executor.map(
                self.download_all_historic_chunks,
                self.config.companies_chunks
            )
            all_historic = pd.concat(historic_results, ignore_index=True)
            all_historic.to_csv(self.config.data_dir / "datasets" / "historic" / "historic.csv")
        all_data: DataFrame = join(all_static, all_historic)
        all_data.to_csv(self.config.data_dir / "datasets" / "all_data.csv")

    def download_all_static_chunks(self, companies: list[str]) -> DataFrame:
        """Downloading all static fields from a list of companies"""
        df: DataFrame = self.download_static_from(companies, self.config.historic_chunks[0])
        for i, chunk in enumerate(self.config.historic_chunks[1:]):
            msg = (
                f"Static download: {companies[0]}-{companies[-1]}: "
                f"Chunk{i + 2}:{len(self.config.historic_chunks)}"
            )
            self.logger.info(msg)
            print(msg)
            new_data: DataFrame = self.download_static_from(companies, chunk)
            df = df.join(new_data.set_index('Instrument'), on='Instrument', how="left")
        df = remove_empty_columns(df)
        df.to_csv(
            self.config.static_dir / f"companies-{companies[0]}-{companies[-1]}.csv",
            index=False
        )
        return df

    def download_static_from(self, companies: list[str], chunk: list[str]) -> DataFrame:
        """Downloading static fields from a list of companies"""
        delay = self.config.retry_delay
        for _ in range(self.config.max_retries):
            try:
                data: DataFrame = pd.DataFrame(
                    ld.get_data(
                        universe=companies,
                        fields=chunk,
                        header_type=HeaderType.NAME
                    )
                )
                return aggregate_static(data)
            except LDError:
                time.sleep(delay)
                delay *= self.config.retry_backoff_multiplier
        self.logger.info(
            "Couldn't download static data for companies: %s and chunks: %s",
            companies, chunk
        )
        raise DataDownloadError(
            f"Connection failed for {companies} companies and chunks {chunk}",
            companies,
            chunk
        )

    def download_all_historic_chunks(self, companies: list[str]) -> DataFrame:
        """Downloading all fields from a company and join them together"""
        df: DataFrame = self.download_historic_from(companies, self.config.historic_chunks[0])
        for i, chunk in enumerate(self.config.historic_chunks[1:]):
            msg = (
                f"History download: {companies[0]} - {companies[-1]}"
                f": Chunk {i + 2}:{len(self.config.historic_chunks)}"
            )
            self.logger.info(msg)
            print(msg)
            new_data: DataFrame = self.download_historic_from(companies, chunk)
            df = df.join(new_data.set_index('Date'), on='Date', how='left', validate='one_to_one')
        df.to_csv(
            self.config.historic_dir / f"companies-{companies[0]}-{companies[-1]}.csv",
            index=False
        )
        return df

    def download_historic_from(self, companies: list[str], chunk: list[str]) -> DataFrame:
        """Downloading content of with time series fields from a company"""
        delay = self.config.retry_delay
        for _ in range(self.config.max_retries):
            try:
                data: DataFrame = ld.get_history(
                    universe=companies,
                    fields=chunk,
                    parameters=self.config.params,
                    header_type=HeaderType.NAME,
                )
                return data
                # return cleaning_history(data)
            except LDError:
                time.sleep(delay)
                delay *= self.config.retry_backoff_multiplier
        self.logger.info(
            "Couldn't download historic data for companies: %s and chunks: %s",
            companies, chunk
        )
        raise DataDownloadError(
            f"Connection failed for {companies} companies and chunks {chunk}",
            companies,
            chunk
        )

    def download_gics_codes(self) -> None:
        """Downloading the GICS sector codes of all companies"""
        gics_codes: DataFrame = ld.get_data(
            universe=self.config.companies,
            fields=["TR.GICSIndustryCode", "TR.GICSIndustry"],
        )
        gics_codes.to_csv(self.config.data_dir / "datasets" / "gics_industry_codes.csv")
