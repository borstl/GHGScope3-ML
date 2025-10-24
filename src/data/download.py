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
from pathlib import Path

from core import Config
from core.exceptions import DataDownloadError
from .cleaning import extract_historic_companies, standardize_historic_collection, \
    standardize_static_collection, extract_static_companies, remove_empty_columns

warnings.simplefilter(action='ignore', category=FutureWarning)


def get_empty_columns_names(csv: str | pathlib.Path) -> list[str]:
    """Returns all empty column names from a data frame as a list"""
    df: DataFrame = pd.read_csv(csv)
    na_df: DataFrame = df.replace("", pd.NA)
    return na_df.columns[df.isna().all()].tolist()

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
        ld.open_session(config_name=str(self.config.lseg_config_file))
        self.session_open = True

    def close_session(self) -> None:
        """Close session for LSEG API"""
        ld.close_session()
        self.session_open = False

    def download_all_frames(self) -> None:
        """Downloading all frames from LSEG database"""
        self.logger.info("Downloading all frames from LSEG database")
        static: dict[str, pd.DataFrame] = {}
        historic: dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(self.config.max_workers) as executor:
            static_results = executor.map(
                self.download_static_from,
                self.config.companies_static_chunks
            )
            for dictionary in static_results:
                static.update(dictionary)
        with ThreadPoolExecutor(self.config.max_workers) as executor:
            historic_results = executor.map(
                self.download_historic_in_chunks,
                self.config.companies_historic_chunks
            )
            for dictionary in historic_results:
                historic.update(dictionary)
        self.merge_static_and_historic(static, historic)

    def merge_static_and_historic(
            self,
            statdict: dict[str, pd.DataFrame],
            histordict: dict[str, pd.DataFrame]
    ) -> None:
        """Merge static and historic dataframes"""
        dataframes: dict[str, pd.DataFrame] = {}
        for instrument, historic_df in histordict.items():
            dataframes[instrument] = historic_df.join(statdict[instrument])
        static_frame: pd.DataFrame = pd.concat(statdict.values())
        static_frame.to_csv(self.config.data_dir / "datasets" / "static" / "static.csv")
        historic_frame: pd.DataFrame = pd.concat(list(histordict.values()))
        historic_frame.to_csv(self.config.data_dir / "datasets" / "historic" / "historic.csv")
        all_data: pd.DataFrame = pd.concat(dataframes.values()).reset_index()
        all_data = remove_empty_columns(all_data)
        all_data.to_csv(self.config.data_dir / "datasets" / "all_data.csv")

    def download_static(self, companies: list[str], chunk: list[str], raw_data_dir: Path) -> dict[str, pd.DataFrame]:
        """Downloading static fields from a list of companies"""
        data: DataFrame = pd.DataFrame(
            ld.get_data(
                universe=companies,
                fields=chunk,
                parameters={'Curn': 'USD',},
                header_type=HeaderType.NAME
            )
        )
        statdict: dict[str, pd.DataFrame] = extract_static_companies(data)
        return standardize_static_collection(statdict, raw_data_dir)

    def download_static_from(
            self, companies: list[str],
            features: list[str],
            raw_data_dir: Path
    ) -> dict[str, pd.DataFrame]:
        """Downloading static fields from a list of companies"""
        delay = self.config.retry_delay
        for _ in range(self.config.max_retries):
            try:
                return self.download_static(companies, features, raw_data_dir)
            except LDError as e:
                msg: str = f"Error downloading static data {e}, retrying in {delay} seconds"
                self.logger.exception(msg)
                print(msg)
                time.sleep(delay)
                delay *= self.config.retry_backoff_multiplier
        exc = DataDownloadError("Static download failed", companies)
        self.logger.exception("Raised DataDownloadError:", exc_info=exc)
        raise exc

    def download_historic_in_chunks(
            self,
            companies: list[str],
            raw_data_dir: Path,
    ) -> dict[str, pd.DataFrame]:
        """Downloading all fields from a company and join them together"""
        i = 0
        msg = f"Downloading Chunk {i+1}:{len(self.config.historic_chunks)}"
        print(msg)
        standardized_data: dict[str, pd.DataFrame] = (
            self.download_historic_from(companies, self.config.historic_chunks[0], raw_data_dir, 0))
        collection: dict[str, pd.DataFrame] = standardized_data
        for i, chunk in enumerate(self.config.historic_chunks[1:]):
            print(msg)
            standardized_data = self.download_historic_from(companies, chunk, raw_data_dir, i + 1)
            for key, new_df in standardized_data.items():
                collection[key] = collection[key].join(new_df)
        return collection

    def download_historic_from(
            self,
            companies: list[str],
            chunk: list[str],
            raw_data_dir: Path,
            iteration
    ) -> dict[str, DataFrame]:
        """Downloading content with time series fields from a company"""
        delay = self.config.retry_delay
        for _ in range(self.config.max_retries):
            try:
                return self.download_historic(companies, chunk, raw_data_dir, iteration)
            except LDError as e:
                msg: str = f"Error downloading historic data {e}, retrying in {delay} seconds"
                self.logger.info(msg)
                print(msg)
                time.sleep(delay)
                delay *= self.config.retry_backoff_multiplier
        exc = DataDownloadError("Historic download failed", companies, chunk)
        self.logger.exception("Historic download failed", exc_info=exc)
        raise exc

    def download_historic(
            self,
            companies: list[str],
            features: list[str],
            raw_data_dir: Path,
            iteration: int
    ) -> dict[str, pd.DataFrame]:
        """Downloading all fields from a company and join them together"""
        data: DataFrame = pd.DataFrame(
            ld.get_history(
                universe=companies,
                fields=features,
                parameters=self.config.params,
                header_type=HeaderType.NAME
            )
        )
        return self.standardize_historic_data(data, raw_data_dir, iteration)

    def standardize_historic_data(
            self,
            df: pd.DataFrame,
            raw_data_dir: Path,
            iteration: int
    ) -> dict[str, pd.DataFrame]:
        """
        Standardize all historical loaded messy dataframes.
        Only works for multiple loaded companies at once.
        """
        return standardize_historic_collection(
            extract_historic_companies(df),
            raw_data_dir,
            iteration
        )

    def download_gics_codes(self) -> None:
        """Downloading the GICS sector codes of all companies"""
        gics_codes: DataFrame = ld.get_data(
            universe=self.config.companies,
            fields=["TR.GICSIndustryCode", "TR.GICSIndustry"],
        )
        gics_codes.to_csv(self.config.data_dir / "datasets" / "gics_industry_codes.csv")
