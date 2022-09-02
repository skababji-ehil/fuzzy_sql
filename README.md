# fuzzy-sql

The package generate random SQL Statements to check the query response from tabular synthetic data against that of tabular real data. Both datasets shall have the same number of columns and rows. 

## Usage:

<!-- 1) From your terminal, install pre-requisite dependencies: 
```
pip install pandas scikit-learn seaborn pdfkit
```
Note that the output report is provided in html format. For Linux machines, the report is provided in pdf as well. For that purpose, Linux users may need to install wkhtmltopdf package using:
```
sudo apt install -y wkhtmltopdf
```
2) Once the above is installed, install fuzzy-sql using:

```
pip install fuzzy-sql
``` -->

1) From your_script.py, import the module fuzzy_sql using:
``` 
from fuzzy_sql import fuzzy_sql
```

2) Use the function fuzzy_sql.fuzz_tabular (...) to generate the random queries. The function constructs the database in your working directory and generates reports in a subfolder under your working directory. The function also  returns a dictionary with all generated queries for further analysis. Fro example, if your data is named X, pass the following arguments in sequence:
* An integer representing the number of required random queries.
* A string representing query type and can be one of four options: "single_agg", "single_fltr","twin_agg", twin_fltr", :twin_aggfltr". The prefixes 'single' and 'twin' refer to whether synthetic data is used in the query generation. For types starting with 'single', the path to synthetic data, if provided, will be ignored. Aggregate (agg) type refers to queries that include GROUP BY clause while Filter (fltr) type refers to queries with WHERE clause without any aggregation. An important type is "twin_aggfltr" where random conditioned (includes WHERE) aggregate (includes GROUP BY) queries are generated. 
* Full file path of real data e.g. "path/to/file/X_real.csv". 
* Full file path of your manually generated metadata that includes each variable description, i.e. continuos, nominal or date. The data shall be passed as json file e.g. "path/to/file/X_meta.json".  See below an example of the json file:
```
{
    "var1":"nominal",
    "var2":"nominal",
    "var3":"continuous",
    "var4":"date",
    "var5":"nominal",
}
```
* Full file path of synthetic data e.g. "path/to/file/X_syn.csv"

Here is an example how to generate 10 queries for a single  dataset:
```
queries=fuzzy_sql.fuzz_tabular(10,,"single_fltr","path/to/file/X_real.csv", "path/to/file/X_metadata.json")
```
Below is another example to generate 100 aggregate queries simualeouldy applied to both real and synthetic input datatsets:

```
queries=fuzzy_sql.fuzz_tabular(100,"twin_agg","path/to/file/X_real.csv", "path/to/file/X_metadata.json", "path/to/file/X_syn.csv")
```

**Note**: Windows users may need to add 'r' before the path string they pass to the function. This will force treating windows backslashes as literal raw character. For instance, pass: r"C:\path\to\file\X_real.csv"

The returned dictionary includes the queries and other information. You may check its keys by typing: queries.keys(). For further information, please refer to help documentation. 