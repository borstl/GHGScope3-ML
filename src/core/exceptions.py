"""Custom Errors"""


class MLProjectError(Exception):
    """Base exception for an ML project"""


class DataDownloadError(MLProjectError):
    """Raised when data download fails"""
    def __init__(self, message: str, companies: list = [], features: list = []):
        self.companies = companies
        self.features = features
        super().__init__(message)


class DataValidationError(MLProjectError):
    """Raised when data validation fails"""


class ConfigurationError(MLProjectError):
    """Raised when configuration is invalid"""
