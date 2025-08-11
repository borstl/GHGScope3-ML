"""
Command Line Interface for ML Project

This module provides a comprehensive CLI for managing data downloads,
analysis, and utility operations.
"""

import logging
from pathlib import Path
from typing import Optional
import click

from core.config import Config
from data.download import LSEGDataDownloader


def setup_logging(log_file: Optional[Path] = None, level: str = "INFO") -> None:
    """Setup logging configuration"""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    if log_file:
        logging.basicConfig(
            filename=log_file,
            encoding="utf-8",
            level=getattr(logging, level.upper()),
            format=log_format
        )
    else:
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format=log_format
        )


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), help='Path to config file')
@click.option('--log-level', '-l', default='INFO',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              help='Logging level')
@click.option('--log-file', '-f', type=click.Path(), help='Log file path')
@click.pass_context
def cli(ctx: click.Context, config: Optional[str], log_level: str, log_file: Optional[str]) -> None:
    """ML Project CLI - Manage data downloads, analysis, and utilities"""
    # Setup logging
    log_path = Path(log_file) if log_file else None
    setup_logging(log_path, log_level)

    # Initialize config and store in context
    try:
        ctx.obj = Config()
        if config:
            # TODO: Implement config file loading
            pass
    except Exception as e:
        click.echo(f"Error initializing config: {e}", err=True)
        ctx.exit(1)



@click.option('--chunk-size', default=10, help='Number of companies per chunk')
@click.option('--max-workers', default=10, help='Maximum number of worker threads')
@click.pass_obj
def all_data(config: Config, chunk_size: int, max_workers: int) -> None:
    """Download all data from LSEG API"""
    try:
        # Update config with CLI options
        config.companies_chunk_size = chunk_size
        config.max_workers = max_workers

        downloader = LSEGDataDownloader(config)

        with downloader:
            click.echo("Downloading all data...")
            downloader.download_all_frames()
            click.echo("All data downloaded successfully!")

    except Exception as e:
        click.echo(f"Error during download: {e}", err=True)
        raise click.Abort()

@cli.command()
@click.pass_obj
def status(config: Config) -> None:
    """Show project status and configuration"""
    try:
        click.echo("ML Project Status")
        click.echo("=" * 50)
        click.echo(f"Project root: {config.project_root}")
        click.echo(f"Data directory: {config.data_dir}")
        click.echo(f"Companies: {len(config.companies)}")
        click.echo(f"Static features: {len(config.static_features)}")
        click.echo(f"Historic features: {len(config.historic_features)}")
        click.echo(f"Chunk sizes: companies={config.companies_chunk_size}, "
                   f"static={config.chunk_size_static}, historic={config.chunk_size_historic}")
        click.echo(f"Max workers: {config.max_workers}")
        click.echo(f"Date range: {config.start_date} to {config.end_date}")

        # Check if data files exist
        click.echo("\nData files:")
        datasets_dir = config.data_dir / "datasets"
        if datasets_dir.exists():
            for subdir in ['static', 'historic']:
                subdir_path = datasets_dir / subdir
                if subdir_path.exists():
                    files = list(subdir_path.glob("*.csv"))
                    click.echo(f"  {subdir}: {len(files)} files")
                else:
                    click.echo(f"  {subdir}: directory not found")
        else:
            click.echo("  datasets directory not found")

    except Exception as e:
        click.echo(f"Error getting status: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.pass_obj
def init(config: Config) -> None:
    """Initialize project directories and files"""
    try:
        click.echo("Initializing project structure...")

        # Create necessary directories
        dirs_to_create = [
            config.data_dir / "datasets" / "static",
            config.data_dir / "datasets" / "historic",
            config.data_dir / "figures",
            config.data_dir / "logs"
        ]

        for dir_path in dirs_to_create:
            dir_path.mkdir(parents=True, exist_ok=True)
            click.echo(f"  Created: {dir_path}")

        # Check if required feature files exist
        required_files = [
            config.companies_file,
            config.static_features_file,
            config.historic_features_file
        ]

        click.echo("\nRequired files:")
        for file_path in required_files:
            if file_path.exists():
                click.echo(f"  ✓ {file_path.name}")
            else:
                click.echo(f"  ✗ {file_path.name} (missing)")

        click.echo("\nProject initialization completed!")

    except Exception as e:
        click.echo(f"Error during initialization: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()
