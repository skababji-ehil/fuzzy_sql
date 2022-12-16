import os
import pandas as pd
import json
from pathlib import Path




def prep_data_for_db(csv_table_path: Path, optional_table_name='None', is_child=False, metadata_dir='None', nrows=None) -> tuple:
    """Reads the input csv file and prepares it for importation into sqlite db for fuzzy-sql analysis. 
    By default, the file name (without extension) will be used as a table name in the database.
    All values are imported as strings. 
    Any "'" found in the values (e.g. '1')  is deleted.
    Any variable (columns) that include dots in their names will be replaced by underscores.
    The function also has the option to generate a template metadata dictionary corresponding to the input data frame. Optionally, the generated template dictionary can be saved to metadata_dir as a json file which can be manually ediated by the user. 

    Args:
        csv_table_path: The input file full path including the file name and csv extension.
        optional_table_name: This is an optional name of the table when imported into the database. The default 'None' will use the csv file name (without extension) as the table's name.
        is_child: A boolean to indicate whether the input table is child or not. This will only impact the generated metadata template. Enter 'False' if the input table is tabular or not a child. 
        metadata_dir: The directory where the metadata file shall be saved. No metadata file is saved if the default value of 'None' is used. 
        n_rows: The number of rows to be read from the input csv file. The default of None will read all the rows in the csv file.

    Returns:
        The pandas dataframe in 'unicode-escape' encoding.  
        The corresponding metadata dictionary. The dictionary is saved in json format to the chosen path as provided in metadata_dir.
    """

    df = pd.read_csv(csv_table_path, encoding='unicode-escape',
                     dtype=str, nrows=nrows)
    #df=pd.read_csv(file_path,encoding = "ISO-8859-1")
    # remove any apostrophe from data
    # In order not to encounter error when reading numeric classes e.g. '1' for Class variable in table C4
    df = df.replace({"'": ""}, regex=True)

    # replace any dot in the column names by underscore
    for i, var in enumerate(list(df.columns)):
        if "." in var:
            df.rename(columns={var: var.replace(".", "_")}, inplace=True)
        else:
            continue

    if optional_table_name == 'None':
        tbl_name = os.path.basename(csv_table_path)
        tbl_name = os.path.splitext(tbl_name)[0]
    else:
        tbl_name = optional_table_name

    metadata = {}
    metadata['table_name'] = tbl_name
    # metadata['tbl_key_name']='Enter string, tuple of strings for concatenated key. Enter Null if table is not linked in relation'
    # metadata['parent_ref']='Enter related parent table name and parent key in tuples. Parent key can be a tuple of variable names for concatenated keys. Enter Null if table is teh root'

    var_name_lst = list(df.dtypes.index)
    var_type_lst = df.dtypes.values
    var_tpls = [[var, str(type)]
                for var, type in zip(var_name_lst, var_type_lst)]
    metadata['table_vars'] = var_tpls

    if is_child:
        parent_details = {
            "enter_first_parent_name": [["enter_parent_key"], ["enter_this_child_key"]],
            "add_another_parent_name": [["enter_parent_composite_key1", "enter_parent_composite_key2"], ["enter_this_child_composite_key1", "enter_this_child_composite_key2"]]
        }
        metadata['parent_details'] = parent_details

    if metadata_dir != 'None':
        fname = os.path.join(metadata_dir, tbl_name+".json")
        if os.path.isfile(fname):
            ans = input(
                'Do you really want to replace the existing JSON metadata file? (y/n)')
            if ans == 'n':
                return df, metadata
        with open(fname, "w") as outfile:
            json.dump(metadata, outfile)

    return df, metadata




def get_vars_to_index(metadata: dict,data: pd.DataFrame, index_vars_types='all', cardinality_cutoff=1) -> list:
    ''' Returns a list of candidate variables to be used for indexing the input data in the database.

    Args:
        metadata: The intended name of the table in the database.
        data: The input data
        index_vars_types: (string): The default 'all' will include all varibles in indexing. If set to 'cat', only categorical varaables as defined in the metadata will be used for indexing. 
        cardinality_cutoff (integer): A number identifying the minimum number of distinct catgeories a varibales needs to have in order to be considered for indexing. A default value of 1 will include all variables.
        
    Returns:
        A list of candidate variables for indexing
    '''
    
    cand_vars=[]
    if index_vars_types=='cat':
        for var_tpl in metadata['table_vars']: 
            if var_tpl[1] in ('UNQID', 'key', 'id', 'unqid'):  # If a unique identifier is listed in the metadata, it shall be included in the canadidate indexing variables
                cand_vars.append(var_tpl[0]) 
        len_data=len(data)
        for var in data.columns:
            cat_var=pd.Categorical(data[var].values)
            card=len(cat_var.categories)
            if card > cardinality_cutoff: 
                cand_vars.append(var)
                
    elif index_vars_types=='all':
        #indexing all variables including contiuous     
        cand_vars=[]
        for var_tpl in metadata['table_vars']: 
            cand_vars.append(var_tpl[0]) 
        
        cand_vars=list(set(cand_vars))
    else: 
        raise Exception("Please choose either 'cat' or 'all' for index_vars_types")
        
    return list(set(cand_vars))




def make_table(table_name: str, df: pd.DataFrame, db_conn: object, indx_vars=[]):
    """Imports the input dataframe into a database table. All dots in the variable names will be replaced by underscores.

    Args:
        table_name: The intended name of the table in the database.
        df: The input data
        db_conn: Database (sqlite3) connection object
        indx_vars: A list of all the variables that need to be indexed in the database. A default value of empty list will result in unindexed table. 
    """

    # # replace any dot in the column names by underscore
    # for i, var in enumerate(list(df.columns)):
    #     if "." in var:
    #         df.rename(columns={var: var.replace(".", "_")}, inplace=True)
    #     else:
    #         continue

    cur = db_conn.cursor()
    cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=(?) ",
                (table_name,))  # sqlite_master holds  the schema of the db including table names
    # If table does not exist (ie returned count is zero), then import the table into db from pandas
    if cur.fetchone()[0] == 0: #if table does not exist
        df.to_sql(table_name, db_conn, index=False)
        print(f'Table {table_name} is created in the database')
        for var in indx_vars:
            cur.execute(f"CREATE INDEX IDX_{table_name}_{var} ON {table_name}({var})")
            print(f'.... The index: IDX_{table_name}_{var} is created for the table: {table_name} in the database')
    else:
        print(f'Table {table_name} already exists in the database')
