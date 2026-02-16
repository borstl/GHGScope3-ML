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
    features_dir: Path = data_dir / "features"
    companies_file: Path = features_dir / "companiesA-Z.txt"
    removed_companies_file: Path = features_dir / "removed-features" / "removed_companies.txt"
    removed_features_file_historic: Path = features_dir / "removed-features" / "removed_time_series_features.txt"

    filtered_dir: Path = dataset_dir / "filtered"
    filtered_dir_static: Path = filtered_dir / "static"
    filtered_dir_historic: Path = filtered_dir / "historic"
    filtered_dir_raw_data: Path = filtered_dir / "raw"
    filtered_features_file_static: Path = features_dir / "filtered_static_featuresA-Z.txt"
    filtered_features_file_historic: Path = features_dir / "filtered_time_series_featuresA-Z.txt"

    eda_filtered_dir: Path = dataset_dir / "eda_filtered"
    eda_dir_raw_data: Path = eda_filtered_dir / "eda_raw"
    eda_dir_static: Path = eda_filtered_dir / "static"
    eda_dir_historic: Path = eda_filtered_dir / "historic"
    eda_features_file_static: Path = features_dir / "eda_filtered_static_featuresA-Z.txt"
    eda_features_file_historic: Path = features_dir / "eda_filtered_time_series_featuresA-Z.txt"

    baseline_dir: Path = dataset_dir / "baseline"
    baseline_static: Path = baseline_dir / "baseline_imputed_static.csv"
    baseline_historic: Path = baseline_dir / "baseline_imputed_historic.csv"

    full_dir: Path = dataset_dir / "full"
    full_dir_raw_data: Path = full_dir / "full_raw"
    full_dir_static: Path = full_dir / "static"
    full_dir_historic: Path = full_dir / "historic"
    full_features_file_static: Path = features_dir / "full_static_features.txt"
    full_features_file_historic: Path = features_dir / "full_time_series_features.txt"

    lseg_config_file: Path = project_root / "Configuration" / "lseg-data.config.json"

    # LSEG Settings
    companies_chunk_size_static: int = 100
    companies_chunk_size_historic: int = 50
    chunk_size_static: int = 840
    chunk_size_historic: int = 12
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
    start_date: str = "2016-01-01"
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
    removed_companies: list[str] = field(default_factory=list)
    removed_features_historic: list[str] = field(default_factory=list)

    # SDate is minus 8 years to reach (2016-01-01),
    # where there is the first TR.UpstreamScope3PurchasedGoodsAndServices reporting
    def __post_init__(self) -> None:
        self.params = {
            "SDate": "0",
            "EDate": "-8",
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
            with open(self.filtered_features_file_static, encoding="utf-8") as f:
                self.static_features = [line.strip() for line in f if line.strip()]
            with open(self.full_features_file_historic, encoding="utf-8") as f:
                self.historic_features = [line.strip() for line in f if line.strip()]
            with open(self.removed_companies_file, "r", encoding="utf-8") as f:
                self.removed_companies = [line.strip() for line in f]
            with open(self.removed_features_file_historic, encoding="utf-8") as f:
                self.removed_features_historic = [line.strip() for line in f]

        except FileNotFoundError as exc:
            raise ConfigurationError("Required file not found") from exc

        self.companies_static_chunks: list[list[str]] = split_in_chunks(
            [company for company in self.companies if self.removed_companies not in self.companies],
            chunk_size=self.companies_chunk_size_static,
            chunk_limit=self.chunk_limit,
        )
        self.companies_historic_chunks: list[list[str]] = split_in_chunks(
            [company for company in self.companies if self.removed_companies not in self.companies],
            chunk_size=self.companies_chunk_size_historic,
            chunk_limit=self.chunk_limit,
        )
        self.static_chunks: list[list[str]] = split_in_chunks(
            self.static_features,
            chunk_size=self.chunk_size_static,
            chunk_limit=self.chunk_limit,
        )
        self.historic_chunks: list[list[str]] = split_in_chunks(
            [f for f in self.historic_features if self.removed_features_historic not in self.historic_features],
            chunk_size=self.chunk_size_historic,
            chunk_limit=self.chunk_limit,
        )
