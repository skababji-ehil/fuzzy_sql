from scipy import stats
from sklearn.preprocessing import StandardScaler
from jsonschema import Draft4Validator
import json
import sqlite3
from textwrap import wrap
import os
from pathlib import Path
import numpy as np
import pandas as pd
import copy
import random
import time
import multiprocessing as mp
from interruptingcow import timeout, Quota
from typing import Union, Tuple


import matplotlib.pylab as plt
import seaborn as sns
sns.set_style("ticks", {'axes.grid': True})


class RndQry():

    def __init__(self, db_conn: object, tbl_names_lst: list,  metadata_lst: list):
        """ Generates a random query for tabular and longitudinal datasets. 

        Args:
            db_conn (object): The connection object of the sqlite database where the data exists.
            tbl_names_lst (list of str) : A list of input table names (strings) in the database to be randomly queried. 
            metadata_lst (list of dict): A list of dictionaries comprising the types of variables and relationships pertaining to each input table. Each dictionary shall conform to the metadata schema.

        """

        assert len(tbl_names_lst) == len(set(tbl_names_lst)
                                         ), "You can not have tables with the same name"
        assert len(tbl_names_lst) == len(
            metadata_lst), "Each input table name shall have its own metadata dictionary."

        #: A boolean for setting the seed. Default is False and hence the query results will vary from one object to another.
        self.fix_seed = False

        # Define random query possible operations
        self.oprtns: dict = {
            'AGG_OPS': {'AVG': 0.5, 'SUM': 0.3, 'MAX': 0.1, 'MIN': 0.1},
            'LOGIC_OPS': {'AND': 0.9, 'OR': 0.1},
            'NOT_STATE': {'0': 0.8, '1': 0.2},
            'CAT_OPS': {'=': 0.25, '<>': 0.25, 'LIKE': 0.15, 'IN': 0.15, 'NOT LIKE': 0.1, 'NOT IN': 0.1},
            'CNT_OPS': {'=': 0.2, '>': 0.1, '<': 0.1, '>=': 0.1, '<=': 0.1, '<>': 0.1, 'BETWEEN': 0.2, 'NOT BETWEEN': 0.1},
            'DT_OPS': {'=': 0.2, '>': 0.1, '<': 0.1, '>=': 0, '<=': 0, '<>': 0.1, 'BETWEEN': 0.2, 'IN': 0.1, 'NOT BETWEEN': 0.1, 'NOT IN': 0.1},
            # Use WHERE or AND with JOIN CLAUSE
            'FILTER_TYPE': {'WHERE': 0.5, 'AND': 0.5},
            'JOIN_TYPE': {'JOIN': 0.5, 'LEFT JOIN': 0.5}
        }

        """
        A dictionary that defines the sets of various operations to be randomly sampled along with their desired discrete probabilities. The dictionary keys are defined below:     

        .. code-block:: console

            'AGG_OPS': The aggregate operations that can be used with any continuous variable,if any, in the SELECT statement. 
            'LOGIC_OPS': The logical operations that can be used to combine conditions in th WHERE clause.
            'NOT STATE': A boolean with '1' indicating that a selected variable is to be negated, i.e. preceded by NOT.
            'CAT_OPS': The comparison operations that can be used with categorical variables. 
            'CNT_OPS': The comparison operations that can be used with continuous variables. 
            'DT_OPS': The comparison operations that can be used with date variables. 
            'FILTER_TYPE': Whether to use WHERE or AND as a filter condition. This is ignored in the case of tabular datasets since WHERE is the only choice.
            'JOIN_TYPE': The type of JOIN in the SQL Lit database.  This is ignored in the case of tabular datasets.
            
        The default options are shown below. For any key, the sum of operation probabilities shall be 1. For instance, in the values below, the probability of sampling 'AVG' is higher than SUM, MIN and MAX, but they all sum up to 1. All these probabilities can be redefined by the user like all other attributes. However, the user needs to make sure that the assigned probabilities will always sum up to 1: 

        .. code-block:: python
        
            oprtns={
            'AGG_OPS':{'AVG':0.25, 'SUM':0.25, 'MAX':0.25, 'MIN':0.25 },
            'LOGIC_OPS':{'AND':0.9,'OR':0.1},
            'NOT_STATE':{'0':0.9, '1':0.1},
            'CAT_OPS':{'=':0.25, '<>':0.25, 'LIKE':0.15, 'IN':0.15, 'NOT LIKE':0.1, 'NOT IN':0.1},
            'CNT_OPS':{'=':0.2, '>':0.1, '<':0.1, '>=':0.1, '<=':0.1, '<>':0.1, 'BETWEEN':0.2, 'NOT BETWEEN':0.1},
            'DT_OPS':{'=':0.2, '>':0.1, '<':0.1, '>=':0, '<=':0, '<>':0.1, 'BETWEEN':0.2, 'IN':0.1, 'NOT BETWEEN':0.1, 'NOT IN':0.1},
            'FILTER_TYPE':{'WHERE':0.5, 'AND':0.5}, 
            'JOIN_TYPE': {'JOIN':0.5, 'LEFT JOIN':0.5}
            }

        
        """

        #: The maximum number of values to be used in the 'IN' operation. You can set that to np.inf if you do not want to enforce any upper bound.
        self.max_in_terms: int = 3

        #: The fixed number of terms (vars) to be used in the GROUPBY clause. Set it to np.inf (default) if you need the number of terms to be randomly selected. If it is set to a larger number than the possible GROUPBY variables, then this number will be ignored.
        self.no_groupby_vars: int = np.inf

        #: The fixed number of terms (vars) to be used in the WHERE clause. Set it to np.inf (default) if you need the number of terms to be randomly selected . If it is set to a larger number than the possible WHERE variables, then this number will be ignored.
        self.no_where_vars: int = np.inf

        #: The fixed number of join terms (tables) to be used in the JOIN clause. It does not include the name of the master parent table (i.e. the table directly following 'FROM; in the SELECT statement). Set it to np.inf to randomly select the number of JOIN terms.
        self.no_join_tables: int = np.inf

        # validate metadata schema
        validator = Draft4Validator(self._get_metdata_schema())
        for i, metadata in enumerate(metadata_lst):
            res = list(validator.iter_errors(metadata))
            if len(res) != 0:
                raise TypeError(
                    f"Table {tbl_names_lst[i]} did not pass schema validation!")

        # validate operations schema
        validator = Draft4Validator(self._get_params_schema())
        res = list(validator.iter_errors(self.oprtns))
        if len(res) != 0:
            raise TypeError(
                "Dictionary of defined operations did not pass schema validation!")

        self._seed_no = 141
        self._db_conn = db_conn
        self._cur = db_conn.cursor()
        self._tbl_name_lst = tbl_names_lst
        self._parent_name_lst, self._child_name_lst, self._sole_name_lst = self._classify_tables(
            tbl_names_lst, metadata_lst)

        # Add to var tuples a valid variable types (ie. CNT, CAT  or DT))
        mod_metadata_lst = []
        for metadata_i in metadata_lst:
            mod_metadata_lst.append(self._map_vars(metadata_i))
        self._metadata_lst = mod_metadata_lst  # A list of dictionaries for each table

        # Construct list of input tables as pandas dataframes for value bags
        mod_tbl_lst = []
        for tbl_name in tbl_names_lst:
            df = pd.read_sql_query(f'SELECT * FROM {tbl_name}', db_conn)
            mod_tbl_lst.append(df)
        self._tbl_lst = mod_tbl_lst

        # Generate value lists
        self._val_lst = []
        for i, tbl_name in enumerate(self._tbl_name_lst):
            val_dict = {}
            val_dict['table_name'] = tbl_name
            for j, var_tpl in enumerate(metadata_lst[i]['table_vars']):
                val_dict[var_tpl[0]] = self._make_val_bag(
                    tbl_name, metadata_lst[i]['table_vars'][j][0])
            self._val_lst.append(val_dict)


# Schema definitions

    def _get_params_schema(self):
        """ Check input operations against schema

        """
        schema = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "properties": {
                    "AGG_OPS": {
                        "type": "object",
                        "properties": {
                            "AVG": {
                                "type": "number"
                            },
                            "SUM": {
                                "type": "number"
                            },
                            "MAX": {
                                "type": "number"
                            },
                            "MIN": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "AVG",
                            "SUM",
                            "MAX",
                            "MIN"
                        ]
                    },
                "LOGIC_OPS": {
                        "type": "object",
                        "properties": {
                            "AND": {
                                "type": "number"
                            },
                            "OR": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "AND",
                            "OR"
                        ]
                        },
                "NOT_STATE": {
                        "type": "object",
                        "properties": {
                            "0": {
                                "type": "number"
                            },
                            "1": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "0",
                            "1"
                        ]
                        },
                "CAT_OPS": {
                        "type": "object",
                        "properties": {
                            "=": {
                                "type": "number"
                            },
                            "<>": {
                                "type": "number"
                            },
                            "LIKE": {
                                "type": "number"
                            },
                            "IN": {
                                "type": "number"
                            },
                            "NOT LIKE": {
                                "type": "number"
                            },
                            "NOT IN": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "=",
                            "<>",
                            "LIKE",
                            "IN",
                            "NOT LIKE",
                            "NOT IN"
                        ]
                        },
                "CNT_OPS": {
                        "type": "object",
                        "properties": {
                            "=": {
                                "type": "number"
                            },
                            ">": {
                                "type": "number"
                            },
                            "<": {
                                "type": "number"
                            },
                            ">=": {
                                "type": "number"
                            },
                            "<=": {
                                "type": "number"
                            },
                            "<>": {
                                "type": "number"
                            },
                            "BETWEEN": {
                                "type": "number"
                            },
                            "NOT BETWEEN": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "=",
                            ">",
                            "<",
                            ">=",
                            "<=",
                            "<>",
                            "BETWEEN",
                            "NOT BETWEEN"
                        ]
                        },
                "DT_OPS": {
                        "type": "object",
                        "properties": {
                            "=": {
                                "type": "number"
                            },
                            ">": {
                                "type": "number"
                            },
                            "<": {
                                "type": "number"
                            },
                            ">=": {
                                "type": "integer"
                            },
                            "<=": {
                                "type": "integer"
                            },
                            "<>": {
                                "type": "number"
                            },
                            "BETWEEN": {
                                "type": "number"
                            },
                            "IN": {
                                "type": "number"
                            },
                            "NOT BETWEEN": {
                                "type": "number"
                            },
                            "NOT IN": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "=",
                            ">",
                            "<",
                            ">=",
                            "<=",
                            "<>",
                            "BETWEEN",
                            "IN",
                            "NOT BETWEEN",
                            "NOT IN"
                        ]
                        },
                "FILTER_TYPE": {
                        "type": "object",
                        "properties": {
                            "WHERE": {
                                "type": "number"
                            },
                            "AND": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "WHERE",
                            "AND"
                        ]
                        },
                "JOIN_TYPE": {
                        "type": "object",
                        "properties": {
                            "JOIN": {
                                "type": "number"
                            },
                            "LEFT JOIN": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "JOIN",
                            "LEFT JOIN"
                        ]
                        }
            },
            "required": [
                "AGG_OPS",
                "LOGIC_OPS",
                "NOT_STATE",
                "CAT_OPS",
                "CNT_OPS",
                "DT_OPS",
                "FILTER_TYPE",
                "JOIN_TYPE"
            ]
        }

        return schema

    def _get_metdata_schema(self):
        schema = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "properties": {
                    "table_name": {
                        "type": "string"
                    },
                "table_vars": {
                        "type": "array",
                        "items": [
                            {
                                "type": "array",
                                "items": [
                                    {
                                        "type": "string"
                                    },
                                    {
                                        "type": "string"
                                    }
                                ]
                            }
                        ]
                },
                "parent_details": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "array",
                            "items": [
                                {
                                    "type": "array",
                                    "items": [
                                        {
                                            "type": "string"
                                        },
                                        {
                                            "type": "string"
                                        }
                                    ]
                                }
                            ]
                        }
                }
            },
            "required": [
                "table_name",
                "table_vars"
            ]
        }
        return schema


########################################## COMMON METHODS  #########################

    def _test_query_time(self, query_expr, max_query_time=5):
        cur = self._cur
        # time.sleep(10) #SMK TEMP
        p = mp.Process(target=cur.execute, name="_test_query_time", args=(query_expr,))
        p.start()
        p.join(max_query_time)  # wait 5 seconds until process terminates
        if p.is_alive():
            p.terminate()
            p.join()
            print('Cant wait any further! I am skipping this one!')  # MK TEMP
            return False  # execution time is too long
        else:
            return True  # execution time is ok

    def _flatten_lst(self, lst):
        return [item for sublist in lst for item in sublist]

    def _remove_sublst(self, lst, sublst):
        assert len(lst) >= len(
            sublst), "The  length of sub-list can not be larger then the length of list"
        sublst = list(set(sublst))
        for x in sublst:
            lst.remove(x)
        return lst

    def _get_tbl_index(self, tbl_name):
        # lookup table index in METADATA_LST
        for i, metadata in enumerate(self._metadata_lst):
            if metadata['table_name'] == tbl_name:
                return i

    def _classify_tables(self, tbl_names_lst, metadata_lst):
        sole_grp = []
        child_grp = []
        parent_grp = []
        for i, (tbl_name, metadata) in enumerate(zip(tbl_names_lst, metadata_lst)):
            if 'parent_details' in metadata:
                child_grp.append(tbl_name)
                parent_grp += list(metadata['parent_details'].keys())
            else:
                sole_grp.append(tbl_name)
        parent_grp = list(set(parent_grp))
        new_sole_grp = []
        for sole in sole_grp:
            if sole not in parent_grp:
                new_sole_grp.append(sole)
        return parent_grp, child_grp, new_sole_grp

    def _get_child_details(self, tbl_name) -> dict:
        # This will return a dictionary for the input table_name (self) for all its childs as keys and list of two lists  of keys for parent and child respectively
        # "child1":[["self_comp_1","self_comp_2"],["child_comp_1",child_comp_2]]
        # "child2":[["self_comp_1","self_comp_2"],["child_comp_1",child_comp_2]] ..etc
        child_name_lst = self._get_tbl_childs(tbl_name)
        child_idx_lst = [self._get_tbl_index(
            child_name) for child_name in child_name_lst]
        child_details = {}
        for child_name, child_idx in zip(child_name_lst, child_idx_lst):
            child_details[child_name] = self._metadata_lst[child_idx]['parent_details'][tbl_name]
        return child_details

    def _get_table_keys(self, tbl_name):
        # if table is sole, this function is not supposed to be called!
        # if table is child, get its get keys from its metadata
        # if table is parent, search for its childs and get its keys from over there.
        # if table is both parent and child, both if's will executed and key_lst will have all its upward and downward keys
        key_lst = []
        tbl_idx = self._get_tbl_index(tbl_name)
        if tbl_name in self._child_name_lst:
            parent_details = self._metadata_lst[tbl_idx]['parent_details']
            for parent in parent_details:
                key_lst.append(parent_details[parent][1])

        if tbl_name in self._parent_name_lst:
            child_details = self._get_child_details(tbl_name)
            for child in child_details:
                key_lst.append(child_details[child][0])

        key_lst = self._flatten_lst(key_lst)
        return key_lst

    def _get_tbl_vars_by_type(self, var_type, tbl_name, drop_key=False):
        # Returns variable names by type (i.e CAT, CNT or DT) for the input table by referring to the corresponding metadata
        tbl_idx = self._get_tbl_index(tbl_name)
        fetched_vars = []
        var_tpls = self._metadata_lst[tbl_idx]['table_vars']
        for tpl in var_tpls:
            if tpl[2] == var_type:
                fetched_vars.append(tpl[0])
        if drop_key and var_type == 'CAT':  # Note that unique key is NOT allowed to be other than CAT type
            keys = self._get_table_keys(tbl_name)
            fetched_vars = self._remove_sublst(fetched_vars, keys)
        return fetched_vars

    def _get_tbl_childs(self, tbl_name: str) -> list:
        # search thru all tables and check if tbl_name exists in parenet
        tbl_childs = []
        # lookup table index in METADATA_LST
        for i, metadata in enumerate(self._metadata_lst):
            if 'parent_details' not in metadata:
                continue
            parents = list(metadata['parent_details'].keys())
            if tbl_name in parents:
                tbl_childs.append(metadata['table_name'])
        return list(set(tbl_childs))

    def _prepend_tbl_name(self, tbl_name, vars_lst):
        # Returns the input var names but prepended with the input table
        var_names = [tbl_name+'.'+x for x in vars_lst]
        return var_names

    def _drop_tbl_name(self, vars_in: list) -> list:
        vars_out = []
        for var in vars_in:
            split = var.split(".")
            vars_out.append(split[1])
        return vars_out

    def _expr_replace_tbl_name(self, expr: str) -> str:
        # replaces the table names of the real dataset by the table names of the synthetic datasets.
        real_name_lst = self._tbl_name_lst
        syn_name_lst = self._syn_tbl_name_lst
        new_expr = copy.deepcopy(expr)
        for real_name, syn_name in zip(real_name_lst, syn_name_lst):
            new_expr = new_expr.replace(real_name, syn_name)
        return new_expr

    def _map_vars(self, metadata: dict) -> dict:
        mod_metadata = copy.deepcopy(metadata)
        for i, var_tpl in enumerate(mod_metadata['table_vars']):
            if var_tpl[1] in ['quantitative', 'continuous', 'interval', 'ratio', 'REAL']:
                mod_metadata['table_vars'][i].append("CNT")
            elif var_tpl[1] in ['qualitative', 'categorical', 'nominal', 'discrete', 'ordinal', 'dichotomous', 'TEXT', 'INTEGER', 'UNQID', 'ID', 'KEY', 'key']:
                mod_metadata['table_vars'][i].append("CAT")
            elif var_tpl[1] in ['date', 'time', 'datetime']:
                mod_metadata['table_vars'][i].append("DT")
            elif var_tpl[1] in ['ignore', 'IGNORE']:
                mod_metadata['table_vars'][i].append("IGN")
            else:
                raise Exception(
                    f"Variable type {var_tpl[1]} in metadata file is not recognized!")
        return mod_metadata

    def _get_var_type(self, table_name: str, var_name: str) -> str:
        # This function gets the input variable type (CAT, CNT or DT) from the metadata and returns it
        # i=self.TBL_NAME_LST.index(table_name) #find table index
        # metadata=self.METADATA_LST[i] #get corresponding metadata dictionary for the table
        for tbl_metdata in self._metadata_lst:
            if tbl_metdata['table_name'] == table_name:
                var_tpl_lst = tbl_metdata['table_vars']
        assert len(
            var_tpl_lst) != 0, f"No variables found for table {table_name}"
        # search for variable in metadata tuples and return it type
        for j, var_tpl in enumerate(var_tpl_lst):
            if var_tpl[0] == var_name:
                return var_tpl_lst[j][2]

    def _make_val_bag(self, table_name: str, var_name: str) -> list:
        i = self._tbl_name_lst.index(table_name)
        df = self._tbl_lst[i]
        vals = df[var_name].values
        vals = [x for x in vals if x == x]  # drop nan
        vals = list(filter(None, vals))  # drop None
        var_type = self._get_var_type(table_name, var_name)
        if var_type == 'CAT':
            # print(table_name,var_name)
            # for the values of categorical variables, add quote to avoid errors in SQL statement
            vals = [
                "'"+str(x)+"'" for x in vals if "'" not in x or '"' not in x]
        vals = vals if len(vals) != 0 else ["'N/A'"]
        return vals

    def _get_val_bag(self, table_name: str, var_name: str) -> str:
        i = self._tbl_name_lst.index(table_name)
        assert self._val_lst[i]['table_name'] == table_name, "Something wrong in table indexing!"
        return self._val_lst[i][var_name]

    def _get_join_on_sub_expr(self, prnt: str, chld: str) -> str:
        # This method deals with both individual and composite keys but it is not tested yet using any composite keys
        chld_tbl_idx = self._get_tbl_index(chld)
        chld_metadata = self._metadata_lst[chld_tbl_idx]
        prnt_key_lst = chld_metadata['parent_details'][prnt][0]
        chld_key_lst = chld_metadata['parent_details'][prnt][1]
        assert len(prnt_key_lst) != 0, "Joining key is not defined!"
        assert len(prnt_key_lst) == len(
            chld_key_lst), "Both connected tables shall have the same number of joining keys"
        prnt_key_lst = self._prepend_tbl_name(prnt, prnt_key_lst)
        chld_key_lst = self._prepend_tbl_name(chld, chld_key_lst)
        if len(prnt_key_lst) == 1:  # single key
            expr = f" ON {prnt_key_lst[0]}={chld_key_lst[0]} "
        else:  # composite key
            expr = " ON "
            for i, (tbl1_key, tbl2_key) in enumerate(zip(prnt_key_lst, chld_key_lst)):
                expr += f"{tbl1_key} = {tbl2_key}"
                if i < len(prnt_key_lst)-1:
                    expr += " AND "
        return expr

    def _make_rnd_from_expr(self) -> str:
        # this function returns an expression joining a master parent (from_tbl) to multiple child, and parent to grandchild..etc
        if self.fix_seed:
            np.random.seed(self._seed_no)
            random.seed(self._seed_no)
        if len(self._sole_name_lst) != 0:
            assert len(
                self._sole_name_lst) == 1, "For tabular fuzzing, you can not have more than one table passed to the class."
            return f" FROM {self._sole_name_lst[0]} ", self._sole_name_lst[0], []
        else:
            # randomly select master parent  (from_tbl)
            parent1 = random.sample(self._parent_name_lst, 1)[0]
            # check the number of childs the master parent has
            child1_lst = self._get_tbl_childs(parent1)
            max_no_join_tbls = len(child1_lst)
            assert max_no_join_tbls >= 1, "Table {from_tbl} does not seem to have any child"
            picked_no_join_tbls = min(random.randint(
                1, max_no_join_tbls), self.no_join_tables)
            join_expr = f" FROM {parent1} "
            child1 = random.choice(child1_lst)  # pick only one child table
            parent = copy.deepcopy(parent1)
            child = copy.deepcopy(child1)
            join_tbl_lst = []
            for i in range(picked_no_join_tbls):
                join_tbl_lst.append(child)
                this_on_expr = self._get_join_on_sub_expr(parent, child)
                join_type = np.random.choice(
                    list(self.oprtns['JOIN_TYPE'].keys()), p=list(self.oprtns['JOIN_TYPE'].values()))
                join_expr += f" {join_type} {child} {this_on_expr}"
                child2_lst = self._get_tbl_childs(child)
                if len(child2_lst) != 0:  # in case there are grandchildren8
                    # make a choice between children and grandchildren
                    child_lst = random.choice(child1_lst, child2_lst)
                    if child_lst == child1_lst:  # picking children
                        parent = copy.deepcopy(parent1)
                        child1_lst.remove(child)
                        child = random.choice(child1_lst)
                    else:  # picking grandchildren
                        parent = copy.deepcopy(child1)
                        child = random.choice(child2_lst)
                else:  # no grandchildren!
                    parent = copy.deepcopy(parent1)
                    # don't pick the same child twice!
                    child1_lst.remove(child)
                    if len(child1_lst) == 0:
                        break
                    child = random.choice(child1_lst)
            return join_expr, parent1, join_tbl_lst

    def _make_query(self, query_exp: str) -> pd.DataFrame:
        cur=self._db_conn.cursor()
        res=cur.execute(query_exp)
        query = res.fetchall()
        query = pd.DataFrame(
            query, columns=[description[0] for description in cur.description])
        cur.close()
        return query

    def _validate_syn_lst(self, syn_tbl_name_lst):
        assert len(syn_tbl_name_lst) == len(
            self._tbl_name_lst), "The number of the synthetic data tables does not match the number of the real data tables!"

        for i, (real_name, syn_name) in enumerate(zip(self._tbl_name_lst, syn_tbl_name_lst)):
            real = self._tbl_lst[i]
            try:
                syn = pd.read_sql_query(
                    f'SELECT * FROM {syn_name}', self._db_conn)
                assert real.shape == syn.shape, f"The synesthetic table {syn_name} does not have the same shape of the real table {real_name}! Please make sure that the real and synthetic lists are ordered properly."
            except:
                raise Exception(
                    f"Table {syn_name} does not exist in database!")
            try:
                assert list(real.columns).sort() == list(syn.columns).sort()
            except:
                raise Exception(
                    f"Table {syn_name} and {real_name} do not have identical variable names!")

        self._syn_tbl_name_lst = syn_tbl_name_lst

    def _make_int_rv(self, max_int, dist='favor_small'):
        xk = np.arange(1, max_int)  # generate supports
        if dist == 'favor_small':
            # sloped down distribution of supports for less terms in where clause
            pk = n_var_probs = xk[::-1]/xk.sum()
        else:
            pk = np.ones(len(xk))/len(xk)  # uniform distribution otherwise
        custom_rv = stats.rv_discrete(name='no_where_terms', values=(xk, pk))
        return custom_rv


##################################### METHODS FOR GENERATING RANDOM AGGREGATE QUERIES #############################


    def _get_rnd_groupby_lst(self, from_tbl, inp_join_tbl_lst, drop_fkey) -> list:
        # returns randomly picked cat vars including the concatenated table name of the real data (ie that is defined in the class)
        # Note: You can group by CAT_VARS and DT_VARS whether from parent or child or both
        join_tbl_lst = copy.deepcopy(inp_join_tbl_lst)
        if self.fix_seed:
            np.random.seed(self._seed_no)
            random.seed(self._seed_no)
            seed = self._seed_no
        else:
            seed = None
        all_catdt_vars = []
        # if len(join_tbl_lst) != 0:
        # possible group-by list includes only from_tbl and join_tbl lists
        join_tbl_lst.append(from_tbl)
        # All table types (ie parent and child) can be used in agg queries as long as they have CAT vars
        for tbl_name in join_tbl_lst:
            dt_vars = self._get_tbl_vars_by_type('DT', tbl_name)
            if drop_fkey:
                cat_vars = self._get_tbl_vars_by_type(
                    'CAT', tbl_name, drop_key=True)
            else:
                cat_vars = self._get_tbl_vars_by_type(
                    'CAT', tbl_name, drop_key=False)
            catdt_vars = cat_vars+dt_vars
            catdt_vars = self._prepend_tbl_name(tbl_name, catdt_vars) if len(
                inp_join_tbl_lst) != 0 else catdt_vars
            all_catdt_vars.append(catdt_vars)
        all_catdt_vars = [
            var for vars in all_catdt_vars for var in vars]  # flatten

        if len(all_catdt_vars) == 1 and len(inp_join_tbl_lst) != 0:
            raise Exception(
                "The only available categorical variable is the JOIN key. Add more categorical or date variables, or set drop_fkey to False in _get_rnd_groupby_lst")

        custom_rv = self._make_int_rv(
            len(all_catdt_vars)+1, dist='favor_small')
        selected_n_vars = custom_rv.rvs(1, random_state=seed)
        #selected_n_vars=selected_n_vars if self.no_groupby_vars==np.nan else min(len(all_catdt_vars),self.no_groupby_vars)
        selected_n_vars = min(random.randint(
            1, len(all_catdt_vars)), self.no_groupby_vars)
        picked_vars = random.sample(all_catdt_vars, selected_n_vars)
        picked_vars = list(set(picked_vars))
        return picked_vars

    def _get_rnd_agg_fntn_terms(self, from_tbl, inp_join_tbl_lst) -> tuple:
        join_tbl_lst = copy.deepcopy(inp_join_tbl_lst)
        if self.fix_seed:
            np.random.seed(self._seed_no)
            random.seed(self._seed_no)
        all_cnt_vars = []
        # if len(join_tbl_lst) != 0:
        # possible list for agg_fntn operand includes only from_tbl and join_tbl lists
        join_tbl_lst.append(from_tbl)
        for tbl_name in join_tbl_lst:
            # drop_key is not used for continuous variable since key is only categorical variable
            cnt_vars = self._get_tbl_vars_by_type(
                'CNT', tbl_name, drop_key=False)
            cnt_vars = self._prepend_tbl_name(tbl_name, cnt_vars) if len(
                inp_join_tbl_lst) != 0 else cnt_vars
            all_cnt_vars.append(cnt_vars)
        if all_cnt_vars==[[]]: #fxining bug when all variables in sole table are nominal 
            all_cnt_vars=[]
        if len(all_cnt_vars) == 0:
            # if there are no continuous variables, return  'None' for picked_log_op and picked_cnt_var
            return 'None', 'None'
        #assert len( all_cnt_vars)!=0, "No continuous variable is available to use it with an Aggregate Function. Please set agg_fnt to False."
        all_cnt_vars = [
            var for vars in all_cnt_vars for var in vars]  # flatten
        picked_cnt_var = np.random.choice(all_cnt_vars)
        picked_log_op = np.random.choice(
            list(self.oprtns['AGG_OPS'].keys()), p=list(self.oprtns['AGG_OPS'].values()))
        return picked_log_op, picked_cnt_var

    def compile_agg_expr(self) -> Tuple[str, list, str, list, tuple]:
        """ Generates random aggregate query expression.

        """

        # from table is the table right after the from clause which can be either a sole or parent table
        from_expr, from_table, join_tbl_lst = self._make_rnd_from_expr()
        groupby_lst = self._get_rnd_groupby_lst(
            from_table, join_tbl_lst, drop_fkey=True)
        expr2_1 = ' GROUP BY '
        expr2_2 = f'{groupby_lst}'
        expr2_2 = expr2_2.replace("[", "")
        expr2_2 = expr2_2.replace("]", "")
        expr2_2 = expr2_2.replace("'", "")
        log_op, cnt_var = self._get_rnd_agg_fntn_terms(
            from_table, join_tbl_lst)
        if log_op != 'None':  # Automatically use aggregate function if there is a returned log_op:
            log_op, cnt_var = self._get_rnd_agg_fntn_terms(
                from_table, join_tbl_lst)
            expr1 = f'SELECT {expr2_2}, COUNT(*), {log_op}({cnt_var}) ' + \
                from_expr
        else:  # Don NOT use aggregate function if the returned log_op in 'None':
            expr1 = f'SELECT {expr2_2}, COUNT(*) ' + from_expr

        return expr1+expr2_1+expr2_2, groupby_lst, from_table, join_tbl_lst, (log_op, cnt_var)

    def make_single_agg_query(self, single_expr: str, groupby_lst: list, from_tbl: str, join_tbl_lst: list, agg_fntn_terms: tuple) -> dict:
        """ Executes a single aggregate query expression and returns the result as a dataframe in a dictionary

        """

        dic = {}
        #single_expr,groupby_lst,from_tbl, join_tbl_lst, agg_fntn_terms=self.compile_agg_expr()
        # print(single_expr) #SMK TEMP
        if len(join_tbl_lst) != 0:  # if table is sole
            groupby_lst = self._drop_tbl_name(groupby_lst)
        else:
            groupby_lst = groupby_lst
        query = self._make_query( single_expr)
        # grpby_vars=self._drop_tbl_name(groupby_lst)
        dic['query'] = query
        dic['query_desc'] = {
            "type": "single_agg",
            "agg_fntn": agg_fntn_terms,
            "grpby_vars": groupby_lst,
            "from_tbl_name": from_tbl,
            "join_tbl_name_lst": join_tbl_lst,
            "sql": single_expr,
            "n_rows": query.shape[0],
            "n_cols": query.shape[1]
        }
        return dic

    def make_twin_agg_query(self, syn_tbl_name_lst: list, real_expr: str, real_groupby_lst: list, real_from_tbl: str, real_join_tbl_lst: list, agg_fntn_terms: tuple) -> dict:
        """ Executes a twin (both for real and synthetic datasets) aggregate query expression and returns the results as dataframes in a dictionary

        """

        self._validate_syn_lst(syn_tbl_name_lst)  # validate syn list
        # real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst,agg_fntn_terms=self.compile_agg_expr()
        # print(real_expr) #SMK TEMP
        # if len(real_join_tbl_lst) != 0: #if table is sole
        #     groupby_lst=self._drop_tbl_name(real_groupby_lst)
        # else:
        #     groupby_lst=real_groupby_lst
        syn_expr = self._expr_replace_tbl_name(real_expr)
        syn_from_tbl = syn_tbl_name_lst[self._get_tbl_index(real_from_tbl)]

        if len(real_join_tbl_lst) != 0:
            syn_join_tbl_lst = [syn_tbl_name_lst[self._get_tbl_index(
                real_tbl_name)] for real_tbl_name in real_join_tbl_lst]
        else:
            syn_join_tbl_lst = []

        query_real = self._make_query(real_expr)
        query_syn = self._make_query(syn_expr)
        # grpby_vars=self._drop_tbl_name(real_grp_lst)
        dic = {}
        dic['query_real'] = query_real
        dic['query_syn'] = query_syn
        dic['query_desc'] = {
            "type": "twin_agg",
            "agg_fntn": agg_fntn_terms,
            "grpby_vars": real_groupby_lst,
            "from_tbl_name_real": real_from_tbl,
            "join_tbl_name_lst_real": real_join_tbl_lst,
            "sql_real": real_expr,
            "n_cols_real": query_real.shape[1],
            "n_rows_real": query_real.shape[0],
            "from_tbl_name_syn": syn_from_tbl,
            "join_tbl_name_lst_syn": syn_join_tbl_lst,
            "sql_syn": syn_expr,
            "n_cols_syn": query_syn.shape[1],
            "n_rows_syn": query_syn.shape[0],
        }
        return dic


##################################### METHODS FOR GENERATING RANDOM FILTER QUERIES #############################


    def _get_tbl_val_dict(self, tbl_name) -> tuple:
        # Thi will return a tuple of the input dictionary name and its corresponding var-val dictionary. You can get the list of all values by checking the dictionary keys.
        for tbl_val_dic in self._val_lst:
            val_dict = copy.deepcopy(tbl_val_dic)
            if tbl_val_dic['table_name'] == tbl_name:
                del val_dict['table_name']
                return (tbl_name, val_dict)

    def _get_tbl_var_tpl_lst(self, tbl_name):
        for tbl_metdata in self._metadata_lst:
            if tbl_metdata['table_name'] == tbl_name:
                return [(tbl_name, var_tpl[0]) for var_tpl in tbl_metdata['table_vars']]

    def _get_tbl_key_tpl(self, tbl_name):
        keys = self._get_table_keys(tbl_name)
        return [(tbl_name, key) for key in keys]

    def _get_rnd_where_expr(self, from_tbl, join_tbl_lst, drop_fkey):
        # use WHERE with mix of CAT, CNT, DT variables from both PARENT and CHILD
        if self.fix_seed:
            np.random.seed(self._seed_no)
            random.seed(self._seed_no)
            seed = self._seed_no
        else:
            seed = None
        all_tbl_vars = self._get_tbl_var_tpl_lst(from_tbl)
        for join_tbl_name in join_tbl_lst:
            all_tbl_vars += self._get_tbl_var_tpl_lst(join_tbl_name)

        if drop_fkey:
            all_tbl_keys = self._get_tbl_key_tpl(from_tbl)
            for join_tbl_name in join_tbl_lst:
                all_tbl_keys += self._get_tbl_key_tpl(join_tbl_name)
            if len(all_tbl_keys) == 1:
                raise Exception(
                    "Only join key is available as a variable!! Add more variables.")
            all_tbl_vars = self._remove_sublst(all_tbl_vars, all_tbl_keys)

        custom_rv = self._make_int_rv(len(all_tbl_vars)+1, dist='favor_small')
        selected_n_vars = custom_rv.rvs(1, random_state=seed)
        # selected_n_vars=selected_n_vars if self.no_where_vars==np.nan else min(len(all_tbl_vars),self.no_where_vars)
        selected_n_vars = min(random.randint(
            1, len(all_tbl_vars)), self.no_where_vars)
        picked_vars = random.sample(all_tbl_vars, selected_n_vars)

        # Get the correct operations and values for the the picked variables
        terms = ""
        log_ops = []
        for idx, (tbl_name, var_name) in enumerate(picked_vars):
            var_type = self._get_var_type(tbl_name, var_name)
            val_bag = self._get_val_bag(tbl_name, var_name)
            assert len(
                val_bag) != 0, f"Variable {var_name} in table {tbl_name} does not have enough values to sample from!"

            # adding NOT modifier to variable name
            not_status = np.random.choice(
                list(self.oprtns['NOT_STATE'].keys()), p=list(self.oprtns['NOT_STATE'].values()))
            not_modifier = 'NOT ' if not_status == '1' else ""

            if var_type == 'CAT':
                var_op = np.random.choice(
                    list(self.oprtns['CAT_OPS'].keys()), p=list(self.oprtns['CAT_OPS'].values()))
                if var_op == 'IN' or var_op == 'NOT IN':
                    no_in_terms = random.randint(
                        2, len(val_bag)) if len(val_bag) > 2 else 2
                    no_in_terms = min(no_in_terms, self.max_in_terms)
                    vals = np.random.choice(val_bag, size=no_in_terms)
                    # This will eliminate double quotes in the list of values used in the IN clause
                    for i, x in enumerate(vals):
                        try:
                            vals[i] = eval(x)
                        except:
                            continue
                    vals = set(vals)
                    vals_str = f"{vals}"
                    vals_str = vals_str.replace("}", ")")
                    vals_str = vals_str.replace("{", "(")
                    if var_op == 'IN':
                        term = f" {not_modifier} {tbl_name}.{var_name} IN "+vals_str + " " if len(
                            join_tbl_lst) != 0 else f" {not_modifier} {var_name} IN "+vals_str + " "
                    else:
                        term = f" {not_modifier} {tbl_name}.{var_name} NOT IN "+vals_str + " " if len(
                            join_tbl_lst) != 0 else f" {not_modifier} {var_name} NOT IN "+vals_str + " "
                else:
                    val = np.random.choice(val_bag)
                    term = f" {not_modifier} {tbl_name}.{var_name} {var_op} {val} " if len(
                        join_tbl_lst) != 0 else f" {not_modifier} {var_name} {var_op} {val} "

            elif var_type == 'CNT':
                var_op = np.random.choice(
                    list(self.oprtns['CNT_OPS'].keys()), p=list(self.oprtns['CNT_OPS'].values()))
                if var_op == 'BETWEEN' or var_op == 'NOT BETWEEN':
                    lower_bound = np.random.choice(val_bag)
                    upper_bound_bag = [x for x in val_bag if x >= lower_bound]
                    upper_bound = np.random.choice(upper_bound_bag)
                    if var_op == 'BETWEEN':
                        term = f" {not_modifier} {tbl_name}.{var_name} BETWEEN {lower_bound} AND {upper_bound} " if len(
                            join_tbl_lst) != 0 else f" {not_modifier} {var_name} BETWEEN {lower_bound} AND {upper_bound} "
                    else:
                        term = f" {not_modifier} {tbl_name}.{var_name} NOT BETWEEN {lower_bound} AND {upper_bound} " if len(
                            join_tbl_lst) != 0 else f" {not_modifier} {var_name} NOT BETWEEN {lower_bound} AND {upper_bound} "
                else:
                    val = np.random.choice(val_bag)
                    term = f" {not_modifier} {tbl_name}.{var_name} {var_op} {val} " if len(
                        join_tbl_lst) != 0 else f" {not_modifier} {var_name} {var_op} {val} "

            elif var_type == 'DT':
                var_op = np.random.choice(
                    list(self.oprtns['DT_OPS'].keys()), p=list(self.oprtns['DT_OPS'].values()))
                if var_op == 'BETWEEN' or var_op == 'NOT BETWEEN':
                    lower_bound = np.random.choice(val_bag)
                    upper_bound_bag = [x for x in val_bag if x >= lower_bound]
                    upper_bound = np.random.choice(upper_bound_bag)
                    if var_op == 'BETWEEN':
                        term = f" {not_modifier} {tbl_name}.{var_name} BETWEEN {lower_bound} AND {upper_bound} " if len(
                            join_tbl_lst) != 0 else f" {not_modifier} {var_name} BETWEEN {lower_bound} AND {upper_bound} "
                    else:
                        term = f" {not_modifier} {tbl_name}.{var_name} NOT BETWEEN {lower_bound} AND {upper_bound} " if len(
                            join_tbl_lst) != 0 else f" {not_modifier} {var_name} NOT BETWEEN {lower_bound} AND {upper_bound} "
                elif var_op == 'IN' or var_op == 'NOT IN':
                    no_in_terms = random.randint(
                        2, len(val_bag)) if len(val_bag) > 2 else 2
                    no_in_terms = min(no_in_terms, self.max_in_terms)
                    vals = np.random.choice(val_bag, size=no_in_terms)
                    # This will eliminate double quotes in the list of values used in the IN clause
                    for i, x in enumerate(vals):
                        try:
                            vals[i] = eval(x)
                        except:
                            continue
                    vals = set(vals)
                    vals_str = f"{vals}"
                    vals_str = vals_str.replace("}", ")")
                    vals_str = vals_str.replace("{", "(")
                    if var_op == 'IN':
                        term = f" {not_modifier} {tbl_name}.{var_name} IN "+vals_str + " " if len(
                            join_tbl_lst) != 0 else f" {not_modifier} {var_name} IN "+vals_str + " "
                    else:
                        term = f" {not_modifier} {tbl_name}.{var_name} NOT IN "+vals_str + " " if len(
                            join_tbl_lst) != 0 else f" {not_modifier} {var_name} NOT IN "+vals_str + " "
                else:
                    val = np.random.choice(val_bag)
                    term = f" {not_modifier} {tbl_name}.{var_name} {var_op} {val} " if len(
                        join_tbl_lst) != 0 else f" {not_modifier} {var_name} {var_op} {val} "
            else:
                raise Exception(
                    f"Can not find {var_name} in the lists of all variables!!")

            terms += term
            if idx < len(picked_vars)-1:
                selected_logic_op = np.random.choice(
                    list(self.oprtns['LOGIC_OPS'].keys()),  p=list(self.oprtns['LOGIC_OPS'].values()))
                terms += selected_logic_op

        return terms

    def compile_fltr_expr(self) -> Tuple[str, str, list]:
        """ Generates random filter query expression.

        """
        from_expr, from_tbl, join_tbl_lst = self._make_rnd_from_expr()
        where_expr = self._get_rnd_where_expr(
            from_tbl, join_tbl_lst, drop_fkey=True)
        if len(join_tbl_lst) != 0:
            fltr_type = np.random.choice(list(self.oprtns['FILTER_TYPE'].keys()), p=list(
                self.oprtns['FILTER_TYPE'].values()))
        else:
            fltr_type = 'WHERE'
        expr = 'SELECT * ' + from_expr + f' {fltr_type} ' + where_expr
        return expr, from_tbl, join_tbl_lst

    def make_single_fltr_query(self, single_expr: str, from_tbl: str, join_tbl_lst: list) -> dict:
        """ Executes a single filter query expression and returns the result as dataframe in a dictionary

        """

        dic = {}
        # single_expr,from_tbl, join_tbl_lst =self.compile_fltr_expr()
        # print(single_expr) #SMK TMP
        query = self._make_query(single_expr)
        dic['query'] = query
        dic['query_desc'] = {
            "type": "single_fltr",
            "from_tbl_name": from_tbl,
            "join_tbl_name_lst": join_tbl_lst,
            "sql": single_expr,
            "n_rows": query.shape[0],
            "n_cols": query.shape[1]
        }
        return dic

    def make_twin_fltr_query(self, syn_tbl_name_lst: list, real_expr: str, real_from_tbl: str, real_join_tbl_lst: list) -> dict:
        """ Executes a twin filter query expression and returns the results as dataframes in a dictionary

        """

        self._validate_syn_lst(syn_tbl_name_lst)  # validate syn list
        # real_expr,real_from_tbl, real_join_tbl_lst =self.compile_fltr_expr()
        # print(real_expr) #SMK TMP
        syn_expr = self._expr_replace_tbl_name(real_expr)
        syn_from_tbl = syn_tbl_name_lst[self._get_tbl_index(real_from_tbl)]
        syn_join_tbl_lst = [syn_tbl_name_lst[self._get_tbl_index(
            real_tbl_name)] for real_tbl_name in real_join_tbl_lst]
        query_real = self._make_query(real_expr)
        query_syn = self._make_query(syn_expr)
        dic = {}
        dic['query_real'] = query_real
        dic['query_syn'] = query_syn
        dic['query_desc'] = {
            "type": "twin_fltr",
            "from_tbl_name_real": real_from_tbl,
            "join_tbl_name_lst_real": real_join_tbl_lst,
            "sql_real": real_expr,
            "n_cols_real": query_real.shape[1],
            "n_rows_real": query_real.shape[0],
            "from_tbl_name_syn": syn_from_tbl,
            "join_tbl_name_lst_syn": syn_join_tbl_lst,
            "sql_syn": syn_expr,
            "n_cols_syn": query_syn.shape[1],
            "n_rows_syn": query_syn.shape[0],
        }
        return dic


##################################### METHODS FOR GENERATING RANDOM FILTER-AGGREGATE QUERIES #############################


    def compile_aggfltr_expr(self) -> Tuple[str, list, str, list, tuple]:
        """ Generates a random aggregate-filter query expression.

        """

        # from table is the table right after the from clause which can be either a sole or parent table
        from_expr, from_table, join_tbl_lst = self._make_rnd_from_expr()
        groupby_lst = self._get_rnd_groupby_lst(
            from_table, join_tbl_lst, drop_fkey=True)
        expr2_1 = ' GROUP BY '
        expr2_2 = f'{groupby_lst}'
        expr2_2 = expr2_2.replace("[", "")
        expr2_2 = expr2_2.replace("]", "")
        expr2_2 = expr2_2.replace("'", "")

        where_expr = self._get_rnd_where_expr(
            from_table, join_tbl_lst, drop_fkey=True)

        if len(join_tbl_lst) != 0:
            fltr_type = np.random.choice(list(self.oprtns['FILTER_TYPE'].keys()), p=list(
                self.oprtns['FILTER_TYPE'].values()))
        else:
            fltr_type = 'WHERE'

        log_op, cnt_var = self._get_rnd_agg_fntn_terms(
            from_table, join_tbl_lst)
        if log_op != 'None':
            expr_a = f'SELECT {expr2_2}, COUNT(*), {log_op}({cnt_var}) ' + \
                from_expr
        else:
            expr_a = f'SELECT {expr2_2}, COUNT(*) ' + from_expr

        expr_b = f' {fltr_type} ' + where_expr
        expr_c = f' GROUP BY {expr2_2}'

        return expr_a+expr_b+expr_c, groupby_lst, from_table, join_tbl_lst, (log_op, cnt_var)

    def make_single_aggfltr_query(self, single_expr: str, groupby_lst: list, from_tbl: str, join_tbl_lst: list, agg_fntn_terms: tuple) -> dict:
        """ Executes a single aggregate-filter query expression and returns the result as a dataframe in a dictionary

        """

        dic = {}
        # single_expr,groupby_lst,from_tbl, join_tbl_lst, agg_fntn_terms=self.compile_aggfltr_expr()
        # print(single_expr) #SMK TEMP
        query = self._make_query(single_expr)
        # grpby_vars=self._drop_tbl_name(groupby_lst)
        dic['query'] = query
        dic['query_desc'] = {
            "type": "single_agg",
            "agg_fntn": agg_fntn_terms,
            "grpby_vars": groupby_lst,
            "from_tbl_name": from_tbl,
            "join_tbl_name_lst": join_tbl_lst,
            "sql": single_expr,
            "n_rows": query.shape[0],
            "n_cols": query.shape[1]
        }
        return dic

    def make_twin_aggfltr_query(self, syn_tbl_name_lst: list, real_expr: str, real_groupby_lst: list, real_from_tbl: str, real_join_tbl_lst: list, agg_fntn_terms: tuple) -> dict:
        """ Executes a twin aggregate-filter query expression and returns the results as dataframes in a dictionary

        """
        
        self._validate_syn_lst(syn_tbl_name_lst)  # validate syn list
        # real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst, agg_fntn_terms=self.compile_aggfltr_expr()
        # print(real_expr) #SMK TEMP

        # if len(real_join_tbl_lst) == 0: #if table is sole
        #     groupby_lst=self._drop_tbl_name(real_groupby_lst)
        # else:
        #     groupby_lst=real_groupby_lst
        syn_expr = self._expr_replace_tbl_name(real_expr)
        syn_from_tbl = syn_tbl_name_lst[self._get_tbl_index(real_from_tbl)]

        if len(real_join_tbl_lst) != 0:
            syn_join_tbl_lst = [syn_tbl_name_lst[self._get_tbl_index(
                real_tbl_name)] for real_tbl_name in real_join_tbl_lst]
        else:
            syn_join_tbl_lst = []

        query_real = self._make_query(real_expr)
        real_col_dic = dict(
            zip(list(query_real.columns[0:len(real_groupby_lst)]), real_groupby_lst))
        query_real.rename(columns=real_col_dic, inplace=True)
        query_syn = self._make_query(syn_expr)
        # No need to rename the table names to match these in the synthetic data since matching processes requires that both real and syn tables have same varibale names.
        syn_col_dic = dict(
            zip(list(query_syn.columns[0:len(real_groupby_lst)]), real_groupby_lst))
        query_syn.rename(columns=syn_col_dic, inplace=True)

        dic = {}

        dic['query_real'] = query_real
        dic['query_syn'] = query_syn
        dic['query_desc'] = {
            "type": "twin_agg",
            "agg_fntn": agg_fntn_terms,
            "grpby_vars": real_groupby_lst,
            "from_tbl_name_real": real_from_tbl,
            "join_tbl_name_lst_real": real_join_tbl_lst,
            "sql_real": real_expr,
            "n_cols_real": query_real.shape[1],
            "n_rows_real": query_real.shape[0],
            "from_tbl_name_syn": syn_from_tbl,
            "join_tbl_name_lst_syn": syn_join_tbl_lst,
            "sql_syn": syn_expr,
            "n_cols_syn": query_syn.shape[1],
            "n_rows_syn": query_syn.shape[0],
        }

        return dic


############################################ Matching records and calculating metrics for Aggregate and Aggregate-Filter queries 


    def _match_queries4agg(self, rnd_query: dict) -> dict:
        assert 'single' not in rnd_query['query_desc']['type'], "This method does not apply to single random queries!"
        assert '_fltr' not in rnd_query['query_desc']['type'], "This method does not apply to filter random queries. It only applies to aggregate queries!"
        matched_rnd_query = {}
        ext_real = rnd_query['query_real']
        ext_syn = rnd_query['query_syn']

        # redefine var names into number to assure smooth sorting
        ext_var_names = list(ext_real.columns)
        ext_var_nmbrs = list(range(ext_real.shape[1]))

        ext_real.columns = ext_var_nmbrs
        ext_syn.columns = ext_var_nmbrs

        i = 2 if rnd_query['query_desc']['agg_fntn'][0] != 'None' else 1
        red_real = ext_real.iloc[:, :-i]  # drop count and agg fntn columns
        red_syn = ext_syn.iloc[:, :-i]
        agg_col_names = ext_var_names[-i:]
        agg_col_nmbrs = ext_var_nmbrs[-i:]
        red_real.columns = ext_var_nmbrs[0:-i]
        red_syn.columns = ext_var_nmbrs[0:-i]

        assert (red_real.columns == red_syn.columns).all(
        ), "Real and synthetic queries can not be matched since they have different variable names"

        in_real_only = red_real.merge(red_syn, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
        del in_real_only['_merge']
        in_real_only[agg_col_nmbrs] = 0
        ext_syn = pd.concat([ext_syn, in_real_only], axis=0, ignore_index=True)
        ext_syn.sort_values(
            ext_var_nmbrs[0:-i], inplace=True, ignore_index=True)

        in_syn_only = red_real.merge(red_syn, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'right_only']
        del in_syn_only['_merge']
        in_syn_only[agg_col_nmbrs] = 0
        ext_real = pd.concat([ext_real, in_syn_only],
                             axis=0, ignore_index=True)
        ext_real.sort_values(
            ext_var_nmbrs[0:-i], inplace=True, ignore_index=True)

        assert len(ext_real) == len(ext_syn)
        ext_real.columns = ext_var_names
        ext_syn.columns = ext_var_names

        matched_rnd_query['query_real'] = ext_real
        matched_rnd_query['query_syn'] = ext_syn
        matched_rnd_query['query_desc'] = rnd_query['query_desc']
        # matched_rnd_query['query_desc']['n_rows_real']=len(ext_real)
        # matched_rnd_query['query_desc']['n_rows_syn']=len(ext_syn)

        return matched_rnd_query

    def gather_metrics4agg(self, matched_rnd_query: dict) -> dict:
        """ Calculates Hellinger and Normalized Euclidean scores for the input random twin queries (i.e. real and synthetic) and updates the input dictionary with the calculated scores. The input queries shall be matched.

         """

        assert 'single' not in matched_rnd_query['query_desc'][
            'type'], "This method does not apply to single random queries!"
        assert '_fltr' not in matched_rnd_query['query_desc'][
            'type'], "This method does not apply to filter random queries. It only applies to aggregate queries!"

        scored_rnd_query = matched_rnd_query
        real = matched_rnd_query['query_real']
        syn = matched_rnd_query['query_syn']
        desc = matched_rnd_query['query_desc']
        # decide the column back index of COUNT header
        cnt_idx = -1 if desc['agg_fntn'] == 'None' or desc['agg_fntn']==('None','None') else -2
        assert real.iloc[:, :cnt_idx].equals(
            syn.iloc[:, :cnt_idx]), "Real and Synthetic tables should be matched!"

        if len(real) != 0 and len(syn) != 0:
            real_probs = real.iloc[:, cnt_idx].astype(
                float)/sum(real.iloc[:, cnt_idx].astype(float))
            syn_probs = syn.iloc[:, cnt_idx].astype(
                float)/sum(syn.iloc[:, cnt_idx].astype(float))
            hlngr_dist = np.sqrt(
                np.sum((np.sqrt(real_probs)-np.sqrt(syn_probs))**2))/np.sqrt(2)
            scored_rnd_query['query_hlngr_score'] = hlngr_dist

            if cnt_idx == -2:
                # count_real, count_syn, agg_real, agg_syn
                pivot = np.concatenate(
                    [[real.iloc[:, cnt_idx], syn.iloc[:, cnt_idx], real.iloc[:, -1], syn.iloc[:, -1]]], axis=1).T
                pivot = pivot.astype(float)
                # remove rows with any nan
                pivot = pivot[~np.isnan(pivot).any(axis=1), :]
                # dropping rows (ie classes) that do not exist in real or syn queries. Unlike, the Hellinger distance, the Euclidean distance is not meant to measure how good the synthetic model is in terms of teh classes generated.
                cndn = (pivot[:, 0] != 0) & (pivot[:, 1] != 0)
                pivot = pivot[cndn, :]
                p = pivot[:, -2]
                q = pivot[:, -1]
                scaler = StandardScaler()
                p_q = (p-q).reshape(-1, 1)
                if len(p_q) != 0:
                    pq_s = scaler.fit_transform((p-q).reshape(-1, 1))
                    res = np.linalg.norm(pq_s, 2)/len(pq_s)
                    scored_rnd_query['query_ecldn_score'] = res
                else:
                    scored_rnd_query['query_ecldn_score'] = np.nan
            else:
                scored_rnd_query['query_ecldn_score'] = np.nan
        else:
            scored_rnd_query['query_hlngr_score'] = np.nan
            scored_rnd_query['query_ecldn_score'] = np.nan

        return scored_rnd_query


################################################## Calculating metrics for Filter queries 

    
    # # rnd_qry.gather_metrics4fltr(rnd_queries[i]) #to call the function
    # def gather_metrics4fltr(self, rnd_query: dict):
    #     real = rnd_query['query_real']
    #     syn = rnd_query['query_syn']
    #     desc = rnd_query['query_desc']
    #     if len(desc['join_tbl_name_lst_real'])==0: #sole table
    #         real_table_name=desc['from_tbl_name_real']
    #         for var in real.columns:
    #             #now check type each variable
    #             # if var is cat - > call hlngr4cat, save hlngr for var
    #             # if var is cnt . > call hlngr4 cnt, save hlngr for var
    #             # return histor list for all var hlngrs along with median and iqr across them all 
    #     else:
    #         for var in real.columns:
    #             # here we extarct the table name and var name inside the loop from the column name using the dot . separtaion 
    #             # if var is cat - > call hlngr4cat, save hlngr for var
    #             # if var is cnt . > call hlngr4 cnt, save hlngr for var
    #             # return histor list for all var hlngrs along with median and iqr across them all 


################################################### SUPPORTING FUNCTIONS ###################

def get_vars_to_index(metadata: dict,data: pd.DataFrame) -> list:
    ''' Returns candidate variables to be used for indexing the input data in the database.

    Args:
        metadata: The intended name of the table in the database.
        data: The input data

    Returns:
        A list of candidate variables for indexing
    '''
    # cand_vars=[]
    # for var_tpl in metadata['table_vars']: 
    #     if var_tpl[1] in ('UNQID', 'key', 'id', 'unqid'):  # If a unique identifier is listed in the metadata, it shall be included in the canadidate indexing variables
    #         cand_vars.append(var_tpl[0]) 

    # len_data=len(data)
    # for var in data.columns:
    #     cat_var=pd.Categorical(data[var].values)
    #     card=len(cat_var.categories)
    #     if card > 20: #only use variable for indexing if its cardinality (no. of distingt classes) is > 20 (if >0 then it means index all variables)
    #         cand_vars.append(var)
            
    #indexing all variables including contiuous     
    cand_vars=[]
    for var_tpl in metadata['table_vars']: 
        cand_vars.append(var_tpl[0]) 
    
    cand_vars=list(set(cand_vars))
    return cand_vars


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


def gen_queries(n_queries: int, db_conn: object, real_tbl_lst: list, metadata_lst: list,  syn_tbl_lst: list, max_query_time=5) -> list:
    ''' The function generates multiple twin random queries of aggregate-filter type. 

    Args:
        n_queries: The required number of queries to be geenrated.
        db_conn: A connection to the sqlite database where all the input real and synthetic data reside.
        real_tbl_lst: A list of real tables to be used for generating the random queries. The list may include related tables.
        metadata_list: A lsit of dictionaries describing the varibales and relations for each input table. A single metadat dictionaries is used for each real table and its counterpart syntheitc table since both real and syntheitc tables shall have identical varibales and relations.
        syn_tbl_lst: A list of synthetic tables to be used for generating the random queries.
        max_query_time: The maximum time in seconds that is allowed to execute a randomly generated query expression before it skips it to the next random expression. 

    Returns: 
        A list of dictionaries where each dictionary includes the query result for real data as a dataframe, the query result for synthetic data as a dataframe, a dictioanry describing the query details, a float represnting the twin query Hellinger distance and another represnting  Euclidean distance, whenever applicable.  
    '''
    queries = []
    k = 0
    while k < n_queries:
        query_obj = RndQry(db_conn, real_tbl_lst, metadata_lst)
        real_expr, real_groupby_lst, real_from_tbl, real_join_tbl_lst, agg_fntn_terms = query_obj.compile_aggfltr_expr()
        
        # if not query_obj._test_query_time(real_expr):
        #     continue
        test_cur=db_conn.cursor()
        try:
            # with db_conn:
            #     db_conn.execute(real_expr)
            with timeout(5,exception=RuntimeError): #timeout is 5 sec
                test_cur.execute(real_expr)
        # except (sqlite3.OperationalError, sqlite3.IntegrityError) as e:
        #     print('Could not complete operation:', e)
        #     continue
        except RuntimeError:
            test_cur.close()
            print('Cant wait any further! I am using INTERRUPTINCOW for skipping this one!')  # MK TEMP
            continue
        
        rnd_query = query_obj.make_twin_aggfltr_query(syn_tbl_lst, real_expr, real_groupby_lst, real_from_tbl, real_join_tbl_lst, agg_fntn_terms)
        matched_query = query_obj._match_queries4agg(rnd_query)
        scored_query = query_obj.gather_metrics4agg(matched_query)
        queries.append(scored_query)
        k += 1
        print('Generated Random Aggregate Filter Query - {} '.format(str(k)))
    return queries


class QryRprt():

    def __init__(self, dataset_table_lst: list, random_queries: dict):
        """ Generates reports and plots for the input random queries. 

        Args:
            dataset_table_lst (list): List of table names that were used to generte the input queries.
            random_queries (dict) : A dictionary comprising multiple number of queries as datasframes with detailed desciption for each query. 

        """

        self.tbl_lst = dataset_table_lst
        self.rnd_queries = random_queries

        self.style = """
            .mystyle {
                font-size: 7pt; 
                font-family: Arial;
                border-collapse: collapse; 
                border: 1px solid silver;
            /*     margin-left: auto;
                margin-right: auto; */
                
            }

            .mystyle td, th {
                padding: 5px;
                text-align: center;
            }

            .mystyle tr:nth-child(even) {
                background: #E0E0E0;
            }

            .mystyle tr:hover {
                background: silver;
                cursor: pointer;
            }


            ul li {
                font-size: 7px;
            }

            p {
                color: navy;
                text-indent: 7px;
            }
        """

        self.start_html = f"<!DOCTYPE html><html><head><style>{self.style}</style></head><body><H1>Random Queries Generated by Fuzzy SQL</H1>"
        self.end_html = "</body></html>"

    def query_to_html(self, query_id: str, rnd_query: dict) -> str:
        assert query_id == 'real' or query_id == 'syn', (
            "query_id shall be either 'real' or 'syn' ")
        html_string = f"<u>SQL statement - {query_id}:</u><br>"
        html_string += rnd_query['query_desc'][f'sql_{query_id}']
        html_string += "<br><br>"
        if len(rnd_query[f'query_{query_id}']) != 0:
            html_string += f"SQL result - {query_id}:<br>"
            html_string += rnd_query[f'query_{query_id}'].head(
                5).to_html(classes='mystyle')
            html_string += "Number of returned records: " + \
                str(rnd_query['query_desc'][f'n_rows_{query_id}'])
        else:
            html_string += f"<H4>No records returned</H4>"
        html_string += "<br><br>"
        return html_string

    def print_html_mltpl(self, output_file: Path):
        with open(output_file, 'w') as f:
            f.write(self.start_html)
            for query in self.rnd_queries:
                f.write(
                    f"<H3>======================= START RANDOM QUERY ======================</H3>")
                f.write(self.query_to_html('real', query))
                f.write(self.query_to_html('syn', query))
                f.write("Hellinger Distance = {:.3f}".format(
                    query['query_hlngr_score']))
                f.write("<br>")
                f.write("Normalized Euclidean Distance = {:.3f}".format(
                    query['query_ecldn_score']))
                f.write(
                    "<H3>************************************************************************************</H3>")
            f.write(self.end_html)

    def calc_stats(self) -> Tuple[dict, dict]:
        #history_arr =history_arr[~np.isnan(history_arr)]
        hlngr_lst = [self.rnd_queries[i]['query_hlngr_score']
                     for i in range(len(self.rnd_queries))]
        ecldn_lst = [self.rnd_queries[i]['query_ecldn_score']
                     for i in range(len(self.rnd_queries))]

        hlngr_lst = [x for x in hlngr_lst if ~np.isnan(x)]
        ecldn_lst = [x for x in ecldn_lst if ~np.isnan(x)]

        if len(hlngr_lst) != 0:
            mean = np.mean(hlngr_lst)
            median = np.median(hlngr_lst)
            stddev = np.sqrt(np.var(hlngr_lst))
        else:
            mean = np.nan
            median = np.nan
            stddev = np.nan
        hlngr_stats = {'mean': mean, 'median': median, 'stddev': stddev}

        if len(ecldn_lst) != 0:
            mean = np.mean(ecldn_lst)
            median = np.median(ecldn_lst)
            stddev = np.sqrt(np.var(ecldn_lst))
        else:
            mean = np.nan
            median = np.nan
            stddev = np.nan
        ecldn_stats = {'mean': mean, 'median': median, 'stddev': stddev}

        return hlngr_stats, ecldn_stats

    def plot_violin(self, type: str, outputfile: str):
        hlngr_stats, ecldn_stats = self.calc_stats()
        if type == 'Hellinger':
            lst = [self.rnd_queries[i]['query_hlngr_score']
                   for i in range(len(self.rnd_queries))]
            stats = hlngr_stats
        elif type == 'Euclidean':
            lst = [self.rnd_queries[i]['query_ecldn_score']
                   for i in range(len(self.rnd_queries))]
            stats = ecldn_stats
        else:
            raise ValueError('type shall be either Hellinger or Euclidean')

        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        sns.violinplot(x=lst, ax=ax)
        # ax.set_xlim(-0.2,1)
        ax.set_xlabel(type + " ( median: {} , mean: {} , std dev: {} ) ".format(
            round(stats['median'], 2), round(stats['mean'], 2), round(stats['stddev'], 2)))
        # ax.set_xticks([0,0.2,0.4,0.6,0.8,1.0])
        fig.suptitle(
            "\n".join(wrap(f'Fuzzy SQL for {self.tbl_lst}')), fontsize=12)
        fig.savefig(outputfile)
        # fig.show()



