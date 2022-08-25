# fuzzy-sql

The package generate random SQL Statements to check the query response from tabular synthetic data against that of tabular real data. Both datasets shall have the same number of columns and rows. 

## Usage:

#### 1)  Make sure your project has the following structure and upload your data files as shown:
```
your_project/
        |———— src/
        |       your_script.py
        |———— data/
        |       |——— real/
        |       |      real_data_name.csv
        |       |——— synthetic/
        |       |       synthetic_data_name.csv
        |       |——— metadata/
        |               real_data_name.json
        |———— runs/

```
#### 2) From your terminal: pip install fuzzy-sql
#### 3) From your_script.py, import the module fuzzy_sql using: from fuzzy_sql import fuzzy_sql
#### 4) Use the function fuzzy_sql.fuzzy_sql(...) to generate the random queries. The function generates reports in a subfolder under the 'runs' folder and returns a dictionary with all generated queries for further analysis. For further information, please refer to function help documentation. 


