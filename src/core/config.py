"""Config Class"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import lseg.data as ld

from core.exceptions import ConfigurationError


def split_in_chunks(
    parameter_list: list[str],
    chunk_size: int,
    chunk_limit: int = 0,
    skipped_chunks: int = 0,
) -> list[list[str]]:
    """Splitting a list in chunks of e.g., 1000 items"""
    chunks: list[list[str]] = []
    for i in range(0, len(parameter_list), chunk_size):
        chunks.append(parameter_list[i : i + chunk_size])
    if chunk_limit > 0 and skipped_chunks > 0:
        return chunks[skipped_chunks:chunk_limit]
    if chunk_limit > 0 >= skipped_chunks:
        return chunks[:chunk_limit]
    if chunk_limit <= 0 < skipped_chunks:
        return chunks[skipped_chunks:]
    return chunks


@dataclass
class Config:
    """Parameters for the LSEG API"""

    # Paths
    project_root: Path = Path(__file__).parent.parent.parent
    data_dir: Path = project_root / "data"
    dataset_dir: Path = data_dir / "datasets"
    static_dir: Path = dataset_dir / "static"
    historic_dir: Path = dataset_dir / "historic"
    raw_data_dir: Path = dataset_dir / "raw"
    companies_file: Path = data_dir / "features" / "companiesA-Z.txt"
    static_features_file: Path = data_dir / "features" / "limited_static_features.txt"
    historic_features_file: Path = (
        data_dir / "features" / "limited_historic_features.txt"
    )
    lseg_config_file: Path = project_root / "Configuration" / "lseg-data.config.json"

    # LSEG Settings
    companies_chunk_size_static: int = 10
    companies_chunk_size_historic: int = 200
    chunk_size_static: int = 900
    chunk_size_historic: int = 740
    skip_chunks: int = 0
    chunk_limit: int = 0
    too_many_requests_delay: int = 0
    max_workers: int = 2
    max_retries: int = 3
    retry_delay: int = 150
    retry_backoff_multiplier: int = 2
    params: dict[str, str] = field(default_factory=dict)
    lseg_api_configurator = ld.get_config()
    # config.set_param("logs.transports.console.enabled", True)
    # config.set_param("logs.level", "debug")
    # config.set_param("logs.transports.file.name", "lseg-data-lib.log")
    lseg_api_configurator.set_param("http.request-timeout", 20_000)

    # Date ranges
    start_date: str = "2010-01-01"
    end_date: str = "2024-12-31"

    # Logging
    log_level: str = "ERROR"
    log_file: Optional[Path] = project_root / "logs" / "download.log"
    # Features
    companies: list[str] = field(default_factory=list)
    company_chunks: list[list[str]] = field(default_factory=list)
    static_features: list[str] = field(default_factory=list)
    static_chunks: list[list[str]] = field(default_factory=list)
    historic_features: list[str] = field(default_factory=list)
    historic_chunks: list[list[str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.params = {
            "SDate": "-14Y",
            "EDate": "0Y",
            "Period": "FY0",
            "Frq": "FY",  # Yearly frequency
            "interval": "yearly",
            "Curn": "USD",
            "EventType": "ALL",
            "Methodology": "InterimSum",
            "ConsolBasis": "Consolidated"
        }

        # Safely load feature files if they exist
        try:
            with open(self.companies_file, encoding="utf-8") as f:
                self.companies = [line.strip() for line in f if line.strip()]
            with open(self.static_features_file, encoding="utf-8") as f:
                self.static_features = [line.strip() for line in f if line.strip()]
            with open(self.historic_features_file, encoding="utf-8") as f:
                self.historic_features = [line.strip() for line in f if line.strip()]
        except FileNotFoundError as exc:
            raise ConfigurationError("Required file not found") from exc

        self.companies_static_chunks: list[list[str]] = split_in_chunks(
            self.companies,
            chunk_size=self.companies_chunk_size_static,
            chunk_limit=self.chunk_limit,
        )
        self.companies_historic_chunks: list[list[str]] = split_in_chunks(
            self.companies,
            chunk_size=self.companies_chunk_size_historic,
            chunk_limit=self.chunk_limit,
        )
        self.static_chunks: list[list[str]] = split_in_chunks(
            self.static_features,
            chunk_size=self.chunk_size_static,
            chunk_limit=self.chunk_limit,
        )
        self.historic_chunks: list[list[str]] = split_in_chunks(
            self.historic_features,
            chunk_size=self.chunk_size_historic,
            chunk_limit=self.chunk_limit,
        )
