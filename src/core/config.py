"""Config Class"""


from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import lseg.data as ld


@dataclass
class Config:
    """Parameters for the LSEG API"""
    # Paths
    project_root: Path = Path(__file__).parent.parent.parent
    data_dir: Path = project_root / "data"
    companies_file: Path = data_dir / "features" / "companies.txt"
    static_features_file: Path = data_dir / "features" / "limited_static_features.txt"
    historic_features_file: Path = data_dir / "features" / "limited_historic_features.txt"

    # LSEG Settings
    companies_chunk_size: int = 10
    chunk_size_static: int = 100
    chunk_size_historic: int = 10
    skip_chunks: int = 0
    chunk_limit: int = 0
    max_workers: int = 10
    max_retries: int = 10
    retry_delay: int = 1
    retry_backoff_multiplier: int = 2
    params: dict = field(default_factory=dict)
    lseg_api_configurator = ld.get_config()
    # config.set_param("logs.transports.console.enabled", True)
    # config.set_param("logs.level", "debug")
    # config.set_param("logs.transports.file.name", "lseg-data-lib.log")
    lseg_api_configurator.set_param("http.request-timeout", 60_000)


    # Date ranges
    start_date: str = "2010-01-01"
    end_date: str = "2024-12-31"

    # Logging
    log_level: str = "INFO"
    log_file: Optional[Path] = None

    # Features
    companies: list[str] = field(default_factory=list)
    static_features: list[str] = field(default_factory=list)
    historic_features: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.params = {
            "SDate": "CY2010",
            "EDate": "CY2024",
            "Period": "FY0",
            "Frq": "CY"  # Yearly frequency
        }
        
        # Safely load feature files if they exist
        try:
            with open(self.companies_file, encoding="utf-8") as f:
                self.companies = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.companies = []
            print(f"Warning: Companies file not found: {self.companies_file}")
            
        try:
            with open(self.static_features_file, encoding="utf-8") as f:
                self.static_features = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.static_features = []
            print(f"Warning: Static features file not found: {self.static_features_file}")
            
        try:
            with open(self.historic_features_file, encoding="utf-8") as f:
                self.historic_features = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.historic_features = []
            print(f"Warning: Historic features file not found: {self.historic_features_file}")
