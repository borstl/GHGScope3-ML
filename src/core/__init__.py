"""
Core module for ML project.

This module contains core functionality including configuration,
exceptions, parameters, and file management.
"""

from .config import Config
from .exceptions import DataDownloadError

__all__ = [
    'Config',
    'DataDownloadError', 
]
