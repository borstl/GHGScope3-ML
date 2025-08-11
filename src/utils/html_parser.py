"""
Parsing the HTML to extract the necessary fields
"""

import html
from pathlib import Path

from bs4 import BeautifulSoup

from core import Config


def parse(html_path: Path, output: Path) -> None:
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
    config = Config()
    parse(
        config.data_dir / "html" / "Financial-Features-Static.html",
        config.data_dir / "features" / "Financial-Features-Static.html"
    )
