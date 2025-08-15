"""Config Class"""
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

import lseg.data as ld

from core.exceptions import ConfigurationError


def split_in_chunks(
        parameter_list: list[str],
        chunk_size: int,
        chunk_limit: int = 0,
        skipped_chunks: int = 0
) -> list[list[str]]:
    """Splitting a list in chunks of e.g., 1000 items"""
    chunks: list[list[str]] = []
    for i in range(0, len(parameter_list), chunk_size):
        chunks.append(parameter_list[i: i + chunk_size])
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
    companies_file: Path = data_dir / "features" / "company.txt"
    static_features_file: Path = data_dir / "features" / "limited_static_features.txt"
    historic_features_file: Path = data_dir / "features" / "limited_historic_features.txt"

    # LSEG Settings
    companies_chunk_size: int = 5
    chunk_size_static: int = 100
    chunk_size_historic: int = 10
    skip_chunks: int = 0
    chunk_limit: int = 3
    max_workers: int = 1
    max_retries: int = 10
    retry_delay: int = 1
    retry_backoff_multiplier: int = 2
    params: dict[str, str] = field(default_factory=dict)
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
    log_file: Optional[Path] = project_root / "logs" / "download.log"
    # Features
    companies: list[str] = field(default_factory=list)
    companie_chunks: list[list[str]] = field(default_factory=list)
    static_features: list[str] = field(default_factory=list)
    static_chunks: list[list[str]] = field(default_factory=list)
    historic_features: list[str] = field(default_factory=list)
    historic_chunks: list[list[str]] = field(default_factory=list)

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
            with open(self.static_features_file, encoding="utf-8") as f:
                self.static_features = [line.strip() for line in f if line.strip()]
            with open(self.historic_features_file, encoding="utf-8") as f:
                self.historic_features = [line.strip() for line in f if line.strip()]
        except FileNotFoundError as exc:
            raise ConfigurationError("Required file not found") from exc

        self.companies_chunks = split_in_chunks(
            self.companies,
            chunk_size=self.companies_chunk_size,
            chunk_limit=self.chunk_limit
        )
        self.static_chunks = split_in_chunks(
            self.static_features,
            chunk_size=self.chunk_size_static,
            chunk_limit=self.chunk_limit
        )
        self.historic_chunks = split_in_chunks(
            self.historic_features,
            chunk_size=self.chunk_size_historic,
            chunk_limit=self.chunk_limit
        )
