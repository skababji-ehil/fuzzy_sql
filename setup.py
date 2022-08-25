from setuptools import setup, find_packages
import os


VERSION = '0.1.0'
DESCRIPTION = 'Generating random queries for tabular data'
LONG_DESCRIPTION = 'A package that generate random SQL Statements to check the query response from tabular synthetic data against that of tabular real data. '

# Setting up
setup(
    name="fuzzy_sql",
    version=VERSION,
    author="Samer Kababji",
    author_email="skababji@ehealthinformation.ca",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[],
    keywords=['python', 'sql', 'random', 'synthetic', 'ml', 'clinical trials'],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)