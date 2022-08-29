# fuzzy-sql

The package generate random SQL Statements to check the query response from tabular synthetic data against that of tabular real data. Both datasets shall have the same number of columns and rows. 

## Usage:

1) From your terminal, install pre-requisite dependencies: 
```
pip install scikit-learn pandas pdfkit seaborn
```
Note that the output report is provided in html format. For Linux machines, the report is provided in pdf as well. For that purpose, Linux users may need to install wkhtmltopdf package using:
```
sudo apt install -y wkhtmltopdf
```
Once the above is installed, install fuzzy-sql using:

```
pip install fuzzy-sql
```

3) From your_script.py, import the module fuzzy_sql using:
``` 
from fuzzy_sql import fuzzy_sql
```

4) Use the function fuzzy_sql.fuzz_tabular (...) to generate the random queries. The function constructs the database in your working directory and generates reports in a subfolder under your working directory. The function also  returns a dictionary with all generated queries for further analysis. Fro example, if your data is named X, pass the following arguments in sequence:
* Number of required random queries
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
* Query type: "single_agg", "single_fltr","twin_agg", twin_fltr" . For the first two options, the synthetic data will be ignored.  

Here is an example how to generate queries for only one dataset:
```
queries=fuzzy_sql.fuzz_tabular(10,"path/to/file/X_real.csv", "path/to/file/X_metadata.json","single_fltr")
```
and for both real and synthetic datatsets along with distance scores:

```
queries=fuzzy_sql.fuzz_tabular(100,"path/to/file/X_real.csv", "path/to/file/X_metadata.json", "path/to/file/X_syn.csv","twin_agg")
```

**Note**: Windows users may need to add 'r' before the path string they pass to the function. This will force treating windows backslashes as literal raw character. For instance, pass: r"C:\path\to\file\X_real.csv"

The returned dictionary includes the queries and other information. You may check its keys by typing: queries.keys(). For further information, please refer to help documentation. 