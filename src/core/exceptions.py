"""Custom Errors"""


class MLProjectError(Exception):
    """Base exception for an ML project"""


class DataDownloadError(MLProjectError):
    """Raised when data download fails"""
    def __init__(self, message: str, companies=None, features=None):
        if features is None:
            features = []
        if companies is None:
            companies = []
        self.companies: list[str] = companies
        self.features: list[str] = features
        super().__init__(message)

    def __str__(self):
        return f"Connection failed for {self.companies} with {self.features}"


class DataValidationError(MLProjectError):
    """Raised when data validation fails"""


#TODO raise when configuration is wrong like empty company list
class ConfigurationError(MLProjectError):
    """Raised when configuration is invalid"""
