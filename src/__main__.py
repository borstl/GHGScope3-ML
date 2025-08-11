"""Entrypoint for running the ML Project CLI"""
import logging

from core import Config
from data import LSEGDataDownloader

if __name__ == "__main__":
    config = Config()
    logging.basicConfig(
        filename=config.log_file,
        encoding="utf-8",
        level=config.log_level,
        format="%(message)s"
    )
    with LSEGDataDownloader(config) as downloader:
        downloader.download_all_frames()
    logging.shutdown()
