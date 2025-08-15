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
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
    )
    with LSEGDataDownloader(config) as downloader:
        downloader.download_all_frames()
    logging.shutdown()
