Usage
=====

Schema
-------
Whether tabular or longitudinal, data is passed to the main class of Fuzzy SQL and other functions as a list of individual tables. Each table shall be accompanied with its metadata dictionary which is typically read from a json file. The metadata dictionary includes the details about each variable in the corresponding table, any parent tables and their joining keys. For tabular data, the description of the parent tables can be simply omitted. Here are two hypothetical examples to clarify that.

* Tabular dataset:

    Assume that a tabular dataset is already imported into the Sqlite database as a table named \ **T** . The table \ **T** has two categorical variables `t1` and `t2` and one continuous variable `t3`.  The metadata dictionary for \ **T**, namely \ **T_metadata** will look like the following:

    .. code-block:: python

        T_metadata={
            "table_name":"T",
            "table_vars":[
                ["t1":"categorical"],
                ["t2":"categorical"],
                ["t3":"continuous"]
                ]
        }


    The table name is passed to any applicable functions in Fuzzy SQL as a list. For instance, consider generating multiple random queries using the function `gen_aggfltr_queries` in :doc:`/functions` , you need to pass the table name as a list of single item, i.e. `real_tbl_lst=['T']`. Similarly, you pass its metadata as a list i.e. `metadata_lst=[T_metadata]`. 
    
    Finally, any synthetic table names shall be passed in an identical way to that is used in passing real table names, i.e. in a form of list of table names. Please refer to :ref:`Code examples` for detailed code examples.


* Longitudinal dataset:
  
    Assume that a longitudinal dataset has a parent table \ **P**  and a child table \ **C** that were already imported under the same names into the Sqlite database. \ **P** has the categorical variables `p1` and the continuous variable `p2`, while \ **C** has the categorical variables `c1`, `c2` and `c3`. The parent and child table are linked using the variables `p1` and `c1` respectively. The metadata dictionary corresponding to \ **P** will look like the following:

        .. code-block:: python

            P_metadata={
                "table_name":"P",
                "table_vars":[
                    ["p1":"categorical"],
                    ["p2":"continuous"]
                    ]
            }

    while the dictionary corresponding to \ **C** will look like:

        .. code-block:: python

            C_metadata={
                "table_name":"C",
                "table_vars":[
                    ["c1":"categorical"],
                    ["c2":"categorical"],
                    ["c3":"categorical"]
                    ],
                "parent_details":{
                    "T":[["p1"],["c1"]] 
                }
            }

    Notice that the above schema allows the addition of several parent tables as well as composite keys to be provided as list entries. 

    The table names are passed to various functions in Fuzzy SQL as a list of tables. For instance, consider generating multiple random queries using the function `gen_aggfltr_queries`, you can pass the dataset table names and dataset_dictionaries as lists of multiple items, i.e. `real_tbl_lst=['P','T']` and `metadata_lst=[P_metadata, C_metadata]` respectively. Please refer to :ref:`Code examples` for detailed code examples. The metadata schema can checked by accessing the class method: `RndQry._get_metdata_schema(self)`.

Data Types
-----------
To ensure the validity of the SQL select statement as interpreted by the database engine, Fuzzy SQL makes a distinction among three basic data types, namely: Categorical, Continuous and Date. Accordingly, if a dataset includes a variable with a different data type, it will be mapped to the proper type as per the table below:

+--------------------------------------------------------------------------------------------------+------------------+
|                                          Input Data Type                                         | Output Data Type |
+==================================================================================================+==================+
| 'qualitative', 'categorical', 'nominal', 'discrete', 'ordinal', 'dichotomous', 'TEXT', ‘INTEGER’ | ‘categorical’    |
+--------------------------------------------------------------------------------------------------+------------------+
| 'quantitative', 'continuous', 'interval', 'ratio',  'REAL'                                       | 'continuous'     |
+--------------------------------------------------------------------------------------------------+------------------+
|    'date', 'time', 'datetime'                                                                    | 'date'           |
+--------------------------------------------------------------------------------------------------+------------------+

.. _Code examples:

Code examples
--------------
Usage is best explained using real examples from various datasets. Please follow the steps below to install and run the examples:

#. Download and unzip the file from the link below: 

    `<https://ehealthinformation-my.sharepoint.com/:u:/g/personal/skababji_ehealthinformation_ca/EV0BRAhYReVIu-Ncyb02LXcBElLMUk20OkHjJYbp7E6z7g?e=aVFaQo>`_

#. Navigate to the folder that contains the Jupyter Notebooks. The folder includes examples for the following three datasets: 

    *  sdgd-C1: Tabular dataset 
    *  cal: Longitudinal dataset with single child
    *  cms: Longitudinal dataset with multiple-child

The sample datasets are provided in `csv` format under the `data` folder. Metadata `json` files are also provided to define the corresponding variable types and data relations for longitudinal datasets. Sample data and metadata files are kept separate for each of the above datasets. 

The first step is to prepare and import the csv files into the databases. All notebooks starting with the format x.1 are used for that purpose. These three notebooks (one for each dataset) are typically run only for the first time to create and setup the databases. A separate database is created for each dataset. The remaining notebooks are for generating SQL random queries after setting up the databases. 

Use Jupyter by typing in your terminal :code:`jupyter notebook` and run the provided notebooks in the proper sequence i.e. first, sett up a database and then generate random queries. 

The notebooks include setting up the necessary environment besides various valuable comments. Moreover, details about adjusting query parameters are provided in one of the files. 
