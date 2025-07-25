"""
Parsing the HTML to extract the necessary fields
"""

import html

from bs4 import BeautifulSoup

STATIC_FIELDS_HTML: str = "../data/html/lseg_static_fields.html"
TIME_SERIES_FIELDS_HTML: str = "../data/html/lseg_time_series_fields.html"
STATIC_OUTPUT: str = "../data/parameter/tr_values_static.txt"
TIME_SERIES_OUTPUT: str = "../data/parameter/tr_values_time_series.txt"


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
    parse(TIME_SERIES_FIELDS_HTML, TIME_SERIES_OUTPUT)
