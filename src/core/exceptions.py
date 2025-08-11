"""Custom Errors"""


class MLProjectError(Exception):
    """Base exception for ML project"""


class DataDownloadError(MLProjectError):
    """Raised when data download fails"""


class DataValidationError(MLProjectError):
    """Raised when data validation fails"""


class ConfigurationError(MLProjectError):
    """Raised when configuration is invalid"""
