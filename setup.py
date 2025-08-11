"""
Setup script for ML Project CLI
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ml-project",
    version="0.1.0",
    author="borstl",
    description="ML Project with LSEG data download and analysis capabilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.13",
    install_requires=[
        "pandas",
        "matplotlib",
        "lseg-data",
        "click",
    ],
    entry_points={
        "console_scripts": [
            "ml-project=src.cli:cli",
        ],
    },
)
