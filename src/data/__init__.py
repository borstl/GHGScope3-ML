"""
Data module for downloading, cleaning, and processing financial data.
"""

from .download import LSEGDataDownloader
from .cleaning import remove_empty_columns, handle_duplicated_rows, resize_to_range_of_years, \
    aggregate_years, attach_multiindex, standardize_historic, extract_historic_companies, \
    standardize_static, standardize_historic_collection, extract_static_companies, aggregate_static

__all__ = [
    'LSEGDataDownloader',
    'remove_empty_columns',
    'handle_duplicated_rows',
    'resize_to_range_of_years',
    'aggregate_years',
    'attach_multiindex',
    'standardize_historic',
    'extract_historic_companies',
    'standardize_static',
    'standardize_historic_collection',
    'extract_static_companies',
    'aggregate_static'
]
