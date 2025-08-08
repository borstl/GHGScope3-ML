"""
Collection of functions to analyse the data
"""

import matplotlib.pyplot as plt
import pandas as pd

INPUT_FILE = "../data/parameter/limited_time_series_features.txt"
REMOVE_FILE = "../data/parameter/removed_time_series_features.txt"
OUTPUT_FILE = "../data/parameter/limited_time_series_features-removed.txt"


def plot_gics_sectors():
    """plot the counts of GICS sectors"""
    dataframe = pd.read_csv("../data/datasets/gics_sector_codes.csv")
    gics_sector = dataframe.loc[:, "GICS Sector Name"]
    counts = gics_sector.value_counts(dropna=False)
    # x_values = [int(x) if not math.isnan(x) else None for x in counts.index]
    # y_values = counts.values
    counts.to_csv("../data/datasets/gics_sector_counts.csv")

    plt.figure(figsize=(10, 8), dpi=150)

    ax = counts.plot(kind="bar")
    ax.set_ylabel("Count")
    ax.set_title("GICS Sector Count")
    for bar_column in ax.containers:
        ax.bar_label(bar_column, labels=counts, fontweight="bold", padding=3)

    plt.xticks(rotation=45, ha="right")
    plt.subplots_adjust(bottom=0.3)

    plt.show()


def plot_gics_industries():
    """plot the counts of GICS sectors"""
    dataframe = pd.read_csv("../data/datasets/gics_industry_codes.csv")
    gics_industry = dataframe.loc[:, "GICS Industry Name"]
    counts = gics_industry.value_counts(dropna=False)
    # x_values = [int(x) if not math.isnan(x) else None for x in counts.index]
    # y_values = counts.values

    plt.figure(figsize=(25, 9), dpi=150)

    ax = counts.plot(kind="bar")
    ax.set_ylabel("Count")
    ax.set_title("GICS Industry Count")
    for bar_column in ax.containers:
        ax.bar_label(bar_column, labels=counts, fontweight="bold", padding=3)

    plt.xticks(rotation=45, ha="right")
    plt.subplots_adjust(bottom=0.3)

    plt.show()


def remove_failed_features_from_list():
    """remove failed features from list and write new file"""
    with open(INPUT_FILE, "r", encoding="utf-8") as f1:
        file_lines = set(line.rstrip() for line in f1)

    with open(REMOVE_FILE, "r", encoding="utf-8") as f2:
        remove_lines = set(line.rstrip() for line in f2)

    filtered_lines = [line for line in file_lines if line not in remove_lines]

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f3:
        for line in filtered_lines:
            f3.write(line + "\n")


if __name__ == "__main__":
    remove_failed_features_from_list()
