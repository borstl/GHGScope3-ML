"""
Collection of functions to analyse the data
"""

import matplotlib.pyplot as plt
import pandas as pd

from core import Config


def plot_gics_sectors() -> None:
    """Plot the counts of GICS sectors"""
    dataframe = pd.read_csv("../data/datasets/gics_sector_codes.csv")
    gics_sector = dataframe.loc[:, "GICS Sector Name"]
    counts = gics_sector.value_counts(dropna=False)

    # Save counts to CSV
    counts.to_csv("../data/datasets/gics_sector_counts.csv")
    # Create the plot
    plt.figure(figsize=(10, 8), dpi=150)
    ax = counts.plot(kind="bar")
    ax.set_ylabel("Count")
    ax.set_title("GICS Sector Count")

    for bar_column in ax.containers:
        ax.bar_label(bar_column, labels=counts, fontweight="bold", padding=3)

    plt.xticks(rotation=45, ha="right")
    plt.subplots_adjust(bottom=0.3)
    plt.show()
        

def plot_gics_industries() -> None:
    """Plot the counts of GICS industries"""
    df = pd.read_csv("../data/datasets/gics_industry_codes.csv")
    gics_industry = df.loc[:, "GICS Industry Name"]
    counts = gics_industry.value_counts(dropna=False)

    # Create the plot
    plt.figure(figsize=(25, 9), dpi=150)
    ax = counts.plot(kind="bar")
    ax.set_ylabel("Count")
    ax.set_title("GICS Industry Count")

    for bar_column in ax.containers:
        ax.bar_label(bar_column, labels=counts, fontweight="bold", padding=3)

    plt.xticks(rotation=45, ha="right")
    plt.subplots_adjust(bottom=0.3)
    plt.show()

    print("GICS Industry plot created")
        

def remove_failed_features_from_list(config: Config) -> None:
    """Remove failed features from list and write new file"""
    input_path = config.data_dir / "parameter" / "limited_time_series_features.txt"
    remove_path = config.data_dir / "parameter" / "removed_time_series_features.txt"
    output_path = config.data_dir / "parameter" / "limited_time_series_features-removed.txt"

    with open(input_path, "r", encoding="utf-8") as f1:
         file_lines = set(line.rstrip() for line in f1)

    with open(remove_path, "r", encoding="utf-8") as f2:
        remove_lines = set(line.rstrip() for line in f2)

    filtered_lines = [line for line in file_lines if line not in remove_lines]

    with open(output_path, "w", encoding="utf-8") as f3:
        for line in filtered_lines:
            f3.write(line + "\n")


if __name__ == "__main__":
    configuration = Config()
    remove_failed_features_from_list(configuration)
    
    # Create plots (uncomment if you want to see them)
    # plot_gics_sectors()
    # plot_gics_industries()
