"""
Collection of functions to analyse the data
"""

import matplotlib.pyplot as plt
import pandas as pd


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


if __name__ == "__main__":
    plot_gics_sectors()
