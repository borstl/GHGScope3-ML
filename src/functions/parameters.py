"""Collection of global parameters"""

COMPANIES_PATH: str = "../data/parameter/company.txt"
STATIC_FIELDS_PATH: str = "../data/parameter/tr_values_static.txt"
HISTORIC_FIELDS_PATH: str = "../data/parameter/tr_values_history.txt"
SAFE_DATA_PATH: str = "../data/datasets/companies/"
companies: list[str]
static_fields: list[str]
historic_fields: list[str]
PARAMS: dict = {
    "SDate": "CY2010",
    "EDate": "CY2024",
    "Period": "FY0",
    "Frq": "CY"  # Yearly frequency
}
SKIP: int = 22_800
CHUNK_SIZE: int = 20
CHUNK_LIMIT: int | None = 1


class Parameter:
    """Class Parameter"""

    def __init__(self):
        """Initialize global variables"""
        with open(COMPANIES_PATH, encoding="utf-8") as f:
            self.companies = [line.strip() for line in f]
        with open(STATIC_FIELDS_PATH, encoding="utf-8") as f:
            self.static_fields = [line.strip() for line in f]
        with open(HISTORIC_FIELDS_PATH, encoding="utf-8") as f:
            self.historic_fields = [line.strip() for line in f]
