# fuzzy-sql

The package generate random SQL Statements to check the query response from tabular synthetic data against that of tabular real data. Both datasets shall have the same number of columns and rows. 

## Usage:

#### 1) From your terminal, install pre-requisite dependencies: 
```
pip install scikit-learn pandas pdfkit seaborn
```
#### 2) From your terminal: 
```
pip install fuzzy-sql
```

#### 3) From your_script.py, import the module fuzzy_sql using:
``` 
from fuzzy_sql import fuzzy_sql
```

#### 4) Use the function fuzzy_sql.make_fuzzy_sql (...) to generate the random queries. The function constructs the database in your working directory and generates reports in a subfolder under your working directory. The function also  returns a dictionary with all generated queries for further analysis. Fro example, if your data is names X, pass the follwing arguments in sequence:
* Number of required random queries
* Full file path of real data e.g. "dir_to_real_data/X_real.csv".
* Full file path of your manually generated metadata that includes each variable description, i.e. continuos, nominal or date. The data shall be passed as json file e.g. "dir_to_metadata/X_meta.json".  See below an example of the json file:
```
{
    "var1":"nominal",
    "var2":"nominal",
    "var3":"continuous",
    "var4":"date",
    "var5":"nominal",
}
```
* Full file path of synthetic data e.g. "dir_to_synthetic_data/X_syn.csv"
* Query type: "single_agg", "single_fltr","twin_agg", twin_fltr" . For the first two options, the synthetic data will be ignored.  

Here is an example how to generate queries for only one dataset:
```
queries=fuzzy_sql.make_fuzzy_sql(10,"full-path-to-file-directory/X_real.csv", "full-path-to-file-directory/X_metadata.json","single_fltr")
```
and for both real and synthetic datatsets along with distance scores:

```
queries=fuzzy_sql.make_fuzzy_sql(100,"full-path-to-file-directory/X_real.csv", "full-path-to-file-directory/X_metadata.json", "full-path-to-file-directory/X_syn.csv","twin_agg")
```
The returned dictionary include the queries and other information. You may check its keys by typing: queries.keys(). For further information, please refer to help documentation. 