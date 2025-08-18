"""
Data module for downloading, cleaning, and processing financial data.
"""

from .download import LSEGDataDownloader
from .cleaning import remove_empty_columns, handle_duplicated_rows, resize_to_range_of_years, \
    aggregate_years, attach_multiindex, standardize_instrument_history, extract_companies, \
    resize_to_range, aggregate_static, clean_static, join_static_and_historic, join_all, \
    duplicate_group, stretch_static, blow_up, concat_companies, join

__all__ = [
    'LSEGDataDownloader',
    'remove_empty_columns',
    'handle_duplicated_rows',
    'resize_to_range_of_years',
    'aggregate_years',
    'attach_multiindex',
    'standardize_instrument_history',
    'extract_companies',
    'resize_to_range',
    'aggregate_static',
    'clean_static',
    'join_static_and_historic',
    'join_all',
    'duplicate_group',
    'stretch_static',
    'blow_up',
    'concat_companies',
    'join'
]
