"""
Parsing the HTML to extract the necessary fields
"""

import html

from bs4 import BeautifulSoup

STATIC_FIELDS_HTML: str = "../data/html/lseg_static_fields.html"
TIME_SERIES_FIELDS_HTML: str = "../data/html/lseg_time_series_fields.html"
ESG_FEATURES_TIMESERIES: str = "../data/html/ESG-Features-TimeSeries.html"
ESG_FEATURES_STATIC: str = "../data/html/ESG-Features-Static.html"
FINANCIAL_FEATURES_TIME_SERIES: str = "../data/html/Financial-Features-TimeSeries.html"
FINANCIAL_FEATURES_STATIC: str = "../data/html/Financial-Features-Static.html"
STATIC_OUTPUT: str = "../data/parameter/tr_values_static.txt"
TIME_SERIES_OUTPUT: str = "../data/parameter/tr_values_time_series.txt"
ESG_FEATURES_STATIC_OUTPUT: str = "../data/parameter/esg_features_static.txt"
ESG_FEATURES_TIMESERIES_OUTPUT: str = "../data/parameter/esg_features_time_series.txt"
FINANCIAL_FEATURES_STATIC_OUTPUT: str = "../data/parameter/financial_features_static.txt"
FINANCIAL_FEATURES_TIME_SERIES_OUTPUT: str = "../data/parameter/financial_features_time_series.txt"


def parse(html_path: str, output: str):
    """Parse the HTML to extract the necessary fields"""
    with open(html_path, encoding="utf-8") as file:
        formatted_html = html.unescape(file.read())
        html_soup = BeautifulSoup(formatted_html, "html.parser")

    spans = html_soup.find_all("span")
    tr_values = set()
    for span in spans:
        text = span.get_text(strip=True)
        if text.startswith("TR."):
            tr_values.add(text)

    with open(output, mode="w", encoding="utf-8") as file:
        file.write("\n".join(tr_values))


if __name__ == "__main__":
    parse(FINANCIAL_FEATURES_TIME_SERIES, FINANCIAL_FEATURES_TIME_SERIES_OUTPUT)
