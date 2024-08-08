# Fuzzy SQL

The package generates semantically and syntactically correct random SELECT SQL Statements. It is developed by <a href="https://www.ehealthinformation.ca/home" target="_blank">EHIL</a> mainly to check the query response from *synthetic* data against that of *real* data. The package supports both tabular and longitudinal datasets. Table shapes, variable names and relations in both real and synthetic datasets shall be identical. 

To install:
```
pip install fuzzy-sql
```

For further details, please refer to the <a href="https://fuzzy-sql.readthedocs.io/en/latest/" target="_blank">Documentation</a>.

Detailed examples comprising three sample datasets are provided under the **examples** folder in the <a href="https://github.com/skababji-ehil/fuzzy_sql" target="_blank">repository</a>. To generate the random queries, you first need to download the sample data by running  **0.0-download_data.ipynb**. You may then proceed with the remaining notebooks to construct the necessary databases and generate the random queries. For details, please refer to the usage/code-examples  subsection in the <a href="https://fuzzy-sql.readthedocs.io/en/latest/" target="_blank">Documentation</a>.
