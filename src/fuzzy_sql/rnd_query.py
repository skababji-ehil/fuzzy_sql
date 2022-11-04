import copy
from http.client import MOVED_PERMANENTLY
from matplotlib.backend_bases import PickEvent
from matplotlib.pyplot import table
import numpy as np
import pandas as pd
import random
from sklearn.preprocessing import StandardScaler
from jsonschema import Draft4Validator
import json
from scipy import stats
import time

#setup default parameters
DEFAULT_PARAMS={
    'AGG_OPS':{'AVG':0.5, 'SUM':0.3, 'MAX':0.1, 'MIN':0.1 },
    'LOGIC_OPS':{'AND':0.9,'OR':0.1},
    'NOT_STATE':{'0':0.8, '1':0.2},
    'CAT_OPS':{'=':0.25, '<>':0.25, 'LIKE':0.15, 'IN':0.15, 'NOT LIKE':0.1, 'NOT IN':0.1},
    'CNT_OPS':{'=':0.2, '>':0.1, '<':0.1, '>=':0.1, '<=':0.1, '<>':0.1, 'BETWEEN':0.2, 'NOT BETWEEN':0.1},
    'DT_OPS':{'=':0.2, '>':0.1, '<':0.1, '>=':0, '<=':0, '<>':0.1, 'BETWEEN':0.2, 'IN':0.1, 'NOT BETWEEN':0.1, 'NOT IN':0.1},
    'FILTER_TYPE':{'WHERE':0.5, 'AND':0.5}, #Use WHERE or AND with JOIN CLAUSE
    'JOIN_TYPE': {'JOIN':0.5, 'LEFT JOIN':0.5}
}


class RND_QUERY():
    """ Generates random queries for tabular and longitudinal datasets. 
    """

    def __init__(self, db_conn: object, tbl_names_lst: list,  metadata_lst: list,params=DEFAULT_PARAMS , seed=False):
        """ 
        Args:
            db_conn: The connection object of the sqlite database where the data exists.
            tbl_names_lst: A list of input table names (strings) in the database to be randomly queried. 
            metadata_lst: A list of dictionaries comprising the types of variables and relationships pertaining to each input table.
            params: A dictionary that includes the set of parameters that are necessary for generating the random queries. 
            seed: If set to True, generated random queries become deterministic. 
        """
        assert len(tbl_names_lst)==len(set(tbl_names_lst)), "You can not have tables with the same name"
        assert len(tbl_names_lst)==len(metadata_lst),"Each input table name shall have its own metadata dictionary."
        
        #validate metadata schema
        # with open('/metadata_schema.json') as f:
        #     schema=json.load(f)
        validator=Draft4Validator(self._get_metdata_schema())
        for i, metadata in enumerate(metadata_lst):
            res=list(validator.iter_errors(metadata))
            if len(res)==0:
                tbl_name=tbl_names_lst[i]
                # print(f"Metadata for table {tbl_name} is valid.") #SMK TEMP
            else:
                print(res) 
        
        validator=Draft4Validator(self._get_params_schema())
        res=list(validator.iter_errors(params))
        if len(res)==0:
            tbl_name=tbl_names_lst[i]
            # print(f"Parameter input is valid.") #SMK TEMP
        else:
            print(res) 

     

        self.SEED=seed
        self.seed_no=141
        
        self.DB_CONN=db_conn
        self.CUR = db_conn.cursor()

    
        self.PARENT_NAME_LST,self.CHILD_NAME_LST,self.SOLE_NAME_LST =self._classify_tables(tbl_names_lst,metadata_lst) 



        self.TBL_NAME_LST=tbl_names_lst

        #Add to var tuples a valid variable types (ie. CNT, CAT  or DT))
        mod_metadata_lst=[]
        for metadata_i in metadata_lst:
            mod_metadata_lst.append(self._map_vars(metadata_i))

        self.METADATA_LST=mod_metadata_lst #A list of dictionaries for each table

        # Aggregate function applies only when there is at least one continuous variable 
        # self.AGG_FNCTN=True if len(self.CNT_VARS['parent'])!=0 or len(self.CNT_VARS['child'])!=0 else False



        # Define random query attributes
        self.ATTRS=params #General attributes that can be set by the user
        

        #Construct list of  dataframes for value bags
        mod_tbl_lst=[]
        for tbl_name in tbl_names_lst:
            df=pd.read_sql_query(f'SELECT * FROM {tbl_name}', db_conn)
            mod_tbl_lst.append(df)  

        self.TBL_LST=mod_tbl_lst

        #Generate value lists
        self.VAL_LST=[]
        for i, tbl_name in enumerate(self.TBL_NAME_LST):
            val_dict={}
            val_dict['table_name']=tbl_name
            for j,var_tpl in enumerate(metadata_lst[i]['table_vars']):
                val_dict[var_tpl[0]]=self._make_val_bag(tbl_name, metadata_lst[i]['table_vars'][j][0])
            self.VAL_LST.append(val_dict)
        

        self.max_in_terms=3 #Maximum number of values for 'in' clause (set to np.inf, if u do not want to enforce any upper bound)
        self.no_groupby_vars=np.nan ##This a fixed number of terms (vars) to be enforced in the GROUPBY clause.Set it to np.inf and the number of terms will be selected randomly. If set to a larger number than the possible GROUPBY variables, then this number will be ignored. 
        self.no_where_vars=np.nan #This a fixed number of terms (vars) to be enforced in the WHERE clause. Set it to np.inf and the number of terms will be selected randomly. If set to a larger number than the possible WHERE variables, then this number will be ignored. 
        self.no_join_tables=np.inf #This is a fixed number of join terms (tables) to be enforced in the JOIN clause. It does not include the name of teh master parent table (coming right after FROM). Set it to np.inf to randomly select the number of JOIN terms. 

############################################ Schema definitions 

    def _get_params_schema(self):
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

    def _test_query_time(self, query_expr):
        cur = self.CUR
        cur.execute(query_expr)
        #time.sleep(10) #SMK TEMP


    def _flatten_lst(self,lst):
        return [item for sublist in lst for item in sublist]


    def _remove_sublst(self, lst, sublst):
        assert len(lst)>= len(sublst), "The  length of sub-list can not be larger then the length of list"
        sublst=list(set(sublst))
        for x in sublst:
            lst.remove(x)
        return lst


    def _get_tbl_index(self,tbl_name):
        #lookup table index in METADATA_LST
        for i,metadata in enumerate(self.METADATA_LST):
            if metadata['table_name']==tbl_name:
                return i


    def _classify_tables(self,tbl_names_lst,metadata_lst):
        sole_grp=[]
        child_grp=[]
        parent_grp=[]
        for i, (tbl_name, metadata) in enumerate(zip(tbl_names_lst,metadata_lst)):
            if 'parent_details' in metadata:
                child_grp.append(tbl_name) 
                parent_grp+=list(metadata['parent_details'].keys()) 
            else:
                sole_grp.append(tbl_name)
        parent_grp=list(set(parent_grp))
        new_sole_grp=[]
        for sole in sole_grp:
            if sole not in parent_grp:
                new_sole_grp.append(sole)
        return parent_grp,child_grp,new_sole_grp


    def _get_child_details(self, tbl_name)-> dict:
        # This will return a dictionary for the input table_name (self) for all its childs as keys and list of two lists  of keys for parent and child respectively 
        #"child1":[["self_comp_1","self_comp_2"],["child_comp_1",child_comp_2]]
        #"child2":[["self_comp_1","self_comp_2"],["child_comp_1",child_comp_2]] ..etc
        child_name_lst=self._get_tbl_childs(tbl_name)
        child_idx_lst=[self._get_tbl_index(child_name) for child_name in child_name_lst]
        child_details={}
        for child_name, child_idx in zip(child_name_lst, child_idx_lst):
            child_details[child_name]=self.METADATA_LST[child_idx]['parent_details'][tbl_name]
        return child_details


    def _get_table_keys(self, tbl_name):
        #if table is sole, this function is not supposed to be called!
        # if table is child, get its get keys from its metadata
        # if table is parent, search for its childs and get its keys from over there.
        # if table is both parent and child, both if's will executed and key_lst will have all its upward and downward keys
        key_lst=[]
        tbl_idx=self._get_tbl_index(tbl_name)
        if tbl_name in self.CHILD_NAME_LST:
            parent_details=self.METADATA_LST[tbl_idx]['parent_details']
            for parent in parent_details:
                key_lst.append(parent_details[parent][1])

        if tbl_name in self.PARENT_NAME_LST: 
            child_details=self._get_child_details(tbl_name)
            for child in child_details:
                key_lst.append(child_details[child][0])
            
        key_lst=self._flatten_lst(key_lst)
        return key_lst
    

    def _get_tbl_vars_by_type(self,var_type, tbl_name, drop_key=False):
        # Returns variables by type (i.e CAT, CNT or DT) for the input table by referring to the corresponding metadata
        tbl_idx=self._get_tbl_index(tbl_name)
        fetched_vars=[]
        var_tpls=self.METADATA_LST[tbl_idx]['table_vars']
        for tpl in var_tpls:
            if tpl[2]==var_type:
                fetched_vars.append(tpl[0])
        if drop_key and var_type=='CAT': #Note that unique key is NOT allowed to be other than CAT type
                keys=self._get_table_keys(tbl_name)
                fetched_vars=self._remove_sublst(fetched_vars,keys)
        return fetched_vars


    def _get_tbl_childs(self,tbl_name: str) -> list:
        #search thru all tables and check if tbl_name exists in parenet 
        tbl_childs=[]
        for i,metadata in enumerate(self.METADATA_LST):#lookup table index in METADATA_LST
            if 'parent_details' not in metadata: 
                continue
            parents=list(metadata['parent_details'].keys())
            if tbl_name in parents:
                tbl_childs.append(metadata['table_name'])
        return list(set(tbl_childs))
    

    def _prepend_tbl_name(self,tbl_name,vars_lst):
        # Returns the input var names but prepended with the input table
        var_names=[tbl_name+'.'+x for x in vars_lst]
        return var_names


    def _drop_tbl_name(self,vars_in: list) ->list:
        vars_out=[]
        for var in vars_in:
            split=var.split(".")
            vars_out.append(split[1])
        return vars_out


    def _expr_replace_tbl_name(self, expr: str)-> str:
        # replaces the table names of the real dataset by the table names of the synthetic datasets.
        real_name_lst=self.TBL_NAME_LST
        syn_name_lst=self.SYN_TBL_NAME_LST
        new_expr=copy.deepcopy(expr)
        for real_name, syn_name in zip(real_name_lst,syn_name_lst):
            new_expr=new_expr.replace(real_name,syn_name)
        return new_expr


    def _map_vars(self,metadata: dict) -> dict:
        mod_metadata=copy.deepcopy(metadata)
        for i, var_tpl in enumerate(mod_metadata['table_vars']):
            if var_tpl[1] in ['quantitative','continuous','interval','ratio', 'REAL']:
                mod_metadata['table_vars'][i].append("CNT")
            elif var_tpl[1] in ['qualitative','categorical','nominal','discrete','ordinal','dichotomous', 'TEXT', 'INTEGER']:
                mod_metadata['table_vars'][i].append("CAT")
            elif var_tpl[1] in ['date','time','datetime']:
                mod_metadata['table_vars'][i].append("DT")
            elif var_tpl[1] in ['ignore','IGNORE']:
                mod_metadata['table_vars'][i].append("IGN")
            else:
                raise Exception(f"Variable type {var_tpl[1]} in metadata file is not recognized!")
        return mod_metadata


    def _get_var_type(self, table_name: str, var_name:str) -> str:
        # This function gets the input variable type (CAT, CNT or DT) from the metadata and returns it
        # i=self.TBL_NAME_LST.index(table_name) #find table index
        # metadata=self.METADATA_LST[i] #get corresponding metadata dictionary for the table
        for tbl_metdata in self.METADATA_LST:
            if tbl_metdata['table_name']==table_name:
                var_tpl_lst=tbl_metdata['table_vars']
        assert len(var_tpl_lst) !=0, f"No variables found for table {table_name}"
        for j,var_tpl in enumerate(var_tpl_lst):#search for variable in metadata tuples and return it type
            if var_tpl[0]==var_name:
                return var_tpl_lst[j][2]


    def _make_val_bag(self,table_name:str, var_name: str) -> list:
        i=self.TBL_NAME_LST.index(table_name)
        df=self.TBL_LST[i]
        vals=df[var_name].values
        vals=[x for x in vals if x==x] #drop nan
        vals=list(filter(None, vals)) #drop None
        var_type=self._get_var_type(table_name, var_name)
        if var_type=='CAT':
            #print(table_name,var_name)
            vals=["'"+str(x)+"'" for x in vals if "'" not in x or '"' not in x] # for the values of categorical variables, add quote to avoid errors in SQL statement
        vals=vals if len(vals)!=0 else ["'N/A'"]
        return vals


    def _get_val_bag(self, table_name: str, var_name:str) -> str:
        i=self.TBL_NAME_LST.index(table_name)
        assert self.VAL_LST[i]['table_name']==table_name, "Something wrong in table indexing!"
        return self.VAL_LST[i][var_name]
  

    def _get_join_on_sub_expr(self,prnt: str,chld: str) -> str:
        #This method deals with both individual and composite keys but it is not tested yet using any composite keys
        chld_tbl_idx=self._get_tbl_index(chld)
        chld_metadata=self.METADATA_LST[chld_tbl_idx]
        prnt_key_lst=chld_metadata['parent_details'][prnt][0]
        chld_key_lst=chld_metadata['parent_details'][prnt][1]
        assert len(prnt_key_lst) !=0, "Joining key is not defined!"
        assert len(prnt_key_lst)==len(chld_key_lst),"Both connected tables shall have the same number of joining keys"
        prnt_key_lst=self._prepend_tbl_name(prnt,prnt_key_lst)
        chld_key_lst=self._prepend_tbl_name(chld,chld_key_lst)
        if len(prnt_key_lst)==1: #single key
            expr=f" ON {prnt_key_lst[0]}={chld_key_lst[0]} "
        else: #composite key
            expr=" ON "
            for i, (tbl1_key, tbl2_key) in enumerate(zip(prnt_key_lst,chld_key_lst)):
                expr+= f"{tbl1_key} = {tbl2_key}"
                if i < len(prnt_key_lst)-1:
                    expr+=" AND "
        return expr



    def _make_rnd_from_expr(self)-> str:
        #this function returns an expression joining a master parent (from_tbl) to multiple child, and parent to grandchild..etc
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)
        if len(self.SOLE_NAME_LST) !=0:
            assert len(self.SOLE_NAME_LST)==1,"For tabular fuzzing, you can not have more than one table passed to the class."
            return f" FROM {self.SOLE_NAME_LST[0]} ", self.SOLE_NAME_LST[0],[]
        else:
            parent1=random.sample(self.PARENT_NAME_LST,1)[0] # randomly select master parent  (from_tbl)
            child1_lst=self._get_tbl_childs(parent1)#check the number of childs the master parent has
            max_no_join_tbls=len(child1_lst)
            assert max_no_join_tbls >=1, "Table {from_tbl} does not seem to have any child"
            picked_no_join_tbls=min(random.randint(1,max_no_join_tbls), self.no_join_tables)
            join_expr=f" FROM {parent1} "
            child1=random.choice(child1_lst) #pick only one child table
            parent=copy.deepcopy(parent1)
            child=copy.deepcopy(child1)
            join_tbl_lst=[]
            for i in range(picked_no_join_tbls):
                join_tbl_lst.append(child)
                this_on_expr=self._get_join_on_sub_expr(parent,child)
                join_type=np.random.choice(list(self.ATTRS['JOIN_TYPE'].keys()), p=list(self.ATTRS['JOIN_TYPE'].values()))
                join_expr+=f" {join_type} {child} {this_on_expr}"
                child2_lst=self._get_tbl_childs(child)
                if len(child2_lst) !=0: #in case there are grandchildren8
                    child_lst=random.choice(child1_lst, child2_lst) #make a choice between children and grandchildren
                    if child_lst==child1_lst: #picking children
                        parent=copy.deepcopy(parent1)
                        child1_lst.remove(child)
                        child=random.choice(child1_lst)
                    else:  #picking grandchildren
                        parent=copy.deepcopy(child1)
                        child=random.choice(child2_lst)
                else: #no grandchildren!
                        parent=copy.deepcopy(parent1)
                        child1_lst.remove(child) #don't pick the same child twice!
                        if len(child1_lst)==0:
                            break
                        child=random.choice(child1_lst) 
            return join_expr, parent1, join_tbl_lst



    def _make_query(self,cur: object, query_exp: str)-> pd.DataFrame:
        cur.execute(query_exp)
        query = cur.fetchall()
        query=pd.DataFrame(query, columns=[description[0] for description in cur.description])
        return query


    def _validate_syn_lst(self,syn_tbl_name_lst):
        assert len(syn_tbl_name_lst)==len(self.TBL_NAME_LST), "The number of the synthetic data tables does not match the number of the real data tables!"

        for i, (real_name,syn_name) in enumerate(zip(self.TBL_NAME_LST, syn_tbl_name_lst)):
            real=self.TBL_LST[i]
            try:
                syn=pd.read_sql_query(f'SELECT * FROM {syn_name}', self.DB_CONN)
                assert real.shape==syn.shape,f"The synesthetic table {syn_name} does not have the same shape of the real table {real_name}! Please make sure that the real and synthetic lists are ordered properly."
            except:
                raise Exception(f"Table {syn_name} does not exist in database!")
            try:
                assert list(real.columns).sort() == list(syn.columns).sort()
            except:
                raise Exception(f"Table {syn_name} and {real_name} do not have identical variable names!")
        
        self.SYN_TBL_NAME_LST=syn_tbl_name_lst


    def _make_int_rv(self, max_int, dist='favor_small'): 
        xk = np.arange(1,max_int)   #generate supports   
        if dist=='favor_small':
            pk = n_var_probs=xk[::-1]/xk.sum() #sloped down distribution of supports for less terms in where clause
        else: 
            pk = np.ones(len(xk))/len(xk) #uniform distribution otherwise
        custom_rv = stats.rv_discrete(name='no_where_terms', values=(xk, pk))
        return custom_rv


##################################### METHODS FOR GENERATING RANDOM AGGREGATE QUERIES #############################


    def _get_rnd_groupby_lst(self,from_tbl, inp_join_tbl_lst, drop_fkey)-> list:
        #returns randomly picked cat vars including the concatenated table name of the real data (ie that is defined in the class)
        #Note: You can group by CAT_VARS and DT_VARS whether from parent or child or both
        join_tbl_lst=copy.deepcopy(inp_join_tbl_lst)
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)
            seed=self.seed_no
        else:
            seed=None
        all_catdt_vars=[]
        # if len(join_tbl_lst) != 0:
        join_tbl_lst.append(from_tbl)#possible group-by list includes only from_tbl and join_tbl lists
        for tbl_name in join_tbl_lst: #All table types (ie parent and child) can be used in agg queries as long as they have CAT vars
            dt_vars=self._get_tbl_vars_by_type('DT',tbl_name)
            if drop_fkey:
                cat_vars=self._get_tbl_vars_by_type('CAT',tbl_name,drop_key=True)
            else:
                cat_vars=self._get_tbl_vars_by_type('CAT',tbl_name, drop_key=False)
            catdt_vars=cat_vars+dt_vars
            catdt_vars=self._prepend_tbl_name(tbl_name,catdt_vars) if len(inp_join_tbl_lst)!=0 else catdt_vars
            all_catdt_vars.append(catdt_vars)
        all_catdt_vars=[var for vars in all_catdt_vars for var in vars] #flatten

        if len(all_catdt_vars)==1:
            raise Exception("The only available categorical variable is the JOIN key. Add more categorical or date variables, or set drop_fkey to False in _get_rnd_groupby_lst")
            
        custom_rv=self._make_int_rv(len(all_catdt_vars)+1, dist='favor_small')
        selected_n_vars=custom_rv.rvs(1, random_state=seed)
        #selected_n_vars=selected_n_vars if self.no_groupby_vars==np.nan else min(len(all_catdt_vars),self.no_groupby_vars)
        selected_n_vars=min(random.randint(1,len(all_catdt_vars)), self.no_groupby_vars)
        picked_vars = random.sample(all_catdt_vars, selected_n_vars)
        picked_vars=list(set(picked_vars))
        return picked_vars


    def _get_rnd_agg_fntn_terms(self,from_tbl,inp_join_tbl_lst)-> tuple:
        join_tbl_lst=copy.deepcopy(inp_join_tbl_lst)
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)
        all_cnt_vars=[]
        # if len(join_tbl_lst) != 0:
        join_tbl_lst.append(from_tbl)#possible list for agg_fntn operand includes only from_tbl and join_tbl lists
        for tbl_name in join_tbl_lst: 
            cnt_vars=self._get_tbl_vars_by_type('CNT',tbl_name,drop_key=False) #drop_key is not used for continuous variable since key is only categorical variable 
            cnt_vars=self._prepend_tbl_name(tbl_name,cnt_vars) if len(inp_join_tbl_lst)!=0 else cnt_vars
            all_cnt_vars.append(cnt_vars)
        if len( all_cnt_vars)==0:
            return 'None', 'None' # if there are no continuous variables, return  'None' for picked_log_op and picked_cnt_var
        #assert len( all_cnt_vars)!=0, "No continuous variable is available to use it with an Aggregate Function. Please set agg_fnt to False."
        all_cnt_vars=[var for vars in all_cnt_vars for var in vars] #flatten
        picked_cnt_var = np.random.choice(all_cnt_vars)
        picked_log_op=np.random.choice(list(self.ATTRS['AGG_OPS'].keys()), p=list(self.ATTRS['AGG_OPS'].values()))
        return picked_log_op,picked_cnt_var


    def compile_agg_expr(self) -> str:
        from_expr, from_table,join_tbl_lst=self._make_rnd_from_expr() #from table is the table right after the from clause which can be either a sole or parent table  
        groupby_lst=self._get_rnd_groupby_lst(from_table,join_tbl_lst, drop_fkey=True)
        expr2_1=' GROUP BY '
        expr2_2=f'{groupby_lst}'
        expr2_2=expr2_2.replace("[","")
        expr2_2=expr2_2.replace("]","")
        expr2_2=expr2_2.replace("'","")
        log_op,cnt_var=self._get_rnd_agg_fntn_terms(from_table,join_tbl_lst)
        if log_op != 'None': # Automatically use aggregate function if there is a returned log_op:
            log_op,cnt_var=self._get_rnd_agg_fntn_terms(from_table,join_tbl_lst)
            expr1=f'SELECT {expr2_2}, COUNT(*), {log_op}({cnt_var}) '+ from_expr        
        else: # Don NOT use aggregate function if the returned log_op in 'None':
            expr1=f'SELECT {expr2_2}, COUNT(*) '+ from_expr   
        return expr1+expr2_1+expr2_2, groupby_lst,from_table,join_tbl_lst, (log_op, cnt_var)

    def make_single_agg_query(self,single_expr,groupby_lst,from_tbl, join_tbl_lst, agg_fntn_terms) -> dict:
        dic={}
        #single_expr,groupby_lst,from_tbl, join_tbl_lst, agg_fntn_terms=self.compile_agg_expr()
        # print(single_expr) #SMK TEMP
        if len(join_tbl_lst) != 0: #if table is sole
            groupby_lst=self._drop_tbl_name(groupby_lst)
        else:
            groupby_lst=groupby_lst
        query=self._make_query(self.CUR, single_expr)
        # grpby_vars=self._drop_tbl_name(groupby_lst)
        dic['query']=query
        dic['query_desc']={
            "type":"single_agg",
            "agg_fntn":agg_fntn_terms,
            "grpby_vars": groupby_lst,
            "from_tbl_name":from_tbl,
            "join_tbl_name_lst":join_tbl_lst,
            "sql":single_expr,
            "n_rows":query.shape[0],
            "n_cols":query.shape[1]
        }
        return dic

    def make_twin_agg_query(self, syn_tbl_name_lst,real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst,agg_fntn_terms):
        self._validate_syn_lst(syn_tbl_name_lst)  #validate syn list
        # real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst,agg_fntn_terms=self.compile_agg_expr()
        # print(real_expr) #SMK TEMP
        # if len(real_join_tbl_lst) != 0: #if table is sole
        #     groupby_lst=self._drop_tbl_name(real_groupby_lst)
        # else:
        #     groupby_lst=real_groupby_lst
        syn_expr=self._expr_replace_tbl_name(real_expr)
        syn_from_tbl=syn_tbl_name_lst[self._get_tbl_index(real_from_tbl)]
        
        if len(real_join_tbl_lst) != 0:
            syn_join_tbl_lst=[syn_tbl_name_lst[self._get_tbl_index(real_tbl_name)] for real_tbl_name in real_join_tbl_lst]
        else:
            syn_join_tbl_lst=[]

        query_real=self._make_query(self.CUR, real_expr)
        query_syn=self._make_query(self.CUR, syn_expr) 
        #grpby_vars=self._drop_tbl_name(real_grp_lst)
        dic={}
        dic['query_real']=query_real
        dic['query_syn']=query_syn
        dic['query_desc']={
            "type":"twin_agg",
            "agg_fntn":agg_fntn_terms,
            "grpby_vars": real_groupby_lst,
            "from_tbl_name_real":real_from_tbl,
            "join_tbl_name_lst_real":real_join_tbl_lst,
            "sql_real":real_expr,
            "n_cols_real":query_real.shape[1],
            "n_rows_real":query_real.shape[0],
            "from_tbl_name_syn":syn_from_tbl,
            "join_tbl_name_lst_syn":syn_join_tbl_lst,
            "sql_syn":syn_expr,
            "n_cols_syn":query_syn.shape[1],
            "n_rows_syn":query_syn.shape[0],
        }
        return dic


##################################### METHODS FOR GENERATING RANDOM FILTER QUERIES #############################


    def _get_tbl_val_dict(self, tbl_name)->tuple:
        #Thi will return a tuple of the input dictionary name and its corresponding var-val dictionary. You can get the list of all values by checking the dictionary keys.
        for tbl_val_dic in self.VAL_LST:
            val_dict=copy.deepcopy(tbl_val_dic)
            if tbl_val_dic['table_name']==tbl_name:
                del val_dict['table_name']
                return (tbl_name, val_dict)


    def _get_tbl_var_tpl_lst(self, tbl_name):
        for tbl_metdata in self.METADATA_LST:
            if tbl_metdata['table_name']==tbl_name:
                return [(tbl_name,var_tpl[0]) for var_tpl in tbl_metdata['table_vars'] ]


    def _get_tbl_key_tpl(self,tbl_name):
            keys=self._get_table_keys(tbl_name)
            return [(tbl_name,key) for key in keys]


    def _get_rnd_where_expr(self,from_tbl,join_tbl_lst, drop_fkey):
        # use WHERE with mix of CAT, CNT, DT variables from both PARENT and CHILD
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)
            seed=self.seed_no
        else:
            seed=None
        all_tbl_vars=self._get_tbl_var_tpl_lst(from_tbl)
        for join_tbl_name in join_tbl_lst:
            all_tbl_vars+=self._get_tbl_var_tpl_lst(join_tbl_name)
        
        if drop_fkey:
            all_tbl_keys=self._get_tbl_key_tpl(from_tbl)
            for join_tbl_name in join_tbl_lst:
                all_tbl_keys+=self._get_tbl_key_tpl(join_tbl_name)
            if len(all_tbl_keys)==1:
                raise Exception("Only join key is available as a variable!! Add more variables.")
            all_tbl_vars=self._remove_sublst(all_tbl_vars,all_tbl_keys)

        custom_rv=self._make_int_rv(len(all_tbl_vars)+1, dist='favor_small')
        selected_n_vars=custom_rv.rvs(1, random_state=seed)
        # selected_n_vars=selected_n_vars if self.no_where_vars==np.nan else min(len(all_tbl_vars),self.no_where_vars)
        selected_n_vars=min(random.randint(1,len(all_tbl_vars)), self.no_where_vars)
        picked_vars = random.sample(all_tbl_vars, selected_n_vars)
        
        # Get the correct operations and values for the the picked variables
        terms=""
        log_ops=[]
        for  idx, (tbl_name, var_name) in enumerate(picked_vars):
            var_type=self._get_var_type(tbl_name, var_name)
            val_bag=self._get_val_bag(tbl_name,var_name)
            assert len(val_bag)!=0, f"Variable {var_name} in table {tbl_name} does not have enough values to sample from!"
            
            #adding NOT modifier to variable name  
            not_status=np.random.choice(list(self.ATTRS['NOT_STATE'].keys()), p=list(self.ATTRS['NOT_STATE'].values()) )
            not_modifier= 'NOT ' if not_status=='1' else ""

            if var_type=='CAT':
                var_op=np.random.choice(list(self.ATTRS['CAT_OPS'].keys()),p=list(self.ATTRS['CAT_OPS'].values()))
                if var_op=='IN' or var_op=='NOT IN' :
                    no_in_terms=random.randint(2,len(val_bag)) if len(val_bag)>2 else 2
                    no_in_terms=min(no_in_terms, self.max_in_terms)
                    vals=np.random.choice(val_bag, size=no_in_terms)
                    for i,x in enumerate(vals): #This will eliminate double quotes in the list of values used in the IN clause
                        try:
                            vals[i]=eval(x)
                        except:
                            continue
                    vals=set(vals) 
                    vals_str=f"{vals}"
                    vals_str=vals_str.replace("}",")")
                    vals_str=vals_str.replace("{","(")
                    if var_op=='IN':
                        term =f" {not_modifier} {tbl_name}.{var_name} IN "+vals_str+ " " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} IN "+vals_str+ " "
                    else:
                        term=f" {not_modifier} {tbl_name}.{var_name} NOT IN "+vals_str+ " " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} NOT IN "+vals_str+ " " 
                else:
                    val=np.random.choice(val_bag)
                    term=f" {not_modifier} {tbl_name}.{var_name} {var_op} {val} " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} {var_op} {val} "

            elif var_type=='CNT':
                var_op=np.random.choice(list(self.ATTRS['CNT_OPS'].keys()),p=list(self.ATTRS['CNT_OPS'].values()))
                if var_op=='BETWEEN' or var_op=='NOT BETWEEN':
                    lower_bound=np.random.choice(val_bag)
                    upper_bound_bag=[x for x in val_bag if x>=lower_bound]
                    upper_bound=np.random.choice(upper_bound_bag)
                    if var_op=='BETWEEN':
                        term=f" {not_modifier} {tbl_name}.{var_name} BETWEEN {lower_bound} AND {upper_bound} " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} BETWEEN {lower_bound} AND {upper_bound} "
                    else:
                        term=f" {not_modifier} {tbl_name}.{var_name} NOT BETWEEN {lower_bound} AND {upper_bound} " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} NOT BETWEEN {lower_bound} AND {upper_bound} "
                else:
                    val=np.random.choice(val_bag)
                    term=f" {not_modifier} {tbl_name}.{var_name} {var_op} {val} " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} {var_op} {val} "

            elif var_type=='DT':
                var_op=np.random.choice(list(self.ATTRS['DT_OPS'].keys()),p=list(self.ATTRS['DT_OPS'].values()))
                if var_op=='BETWEEN' or var_op=='NOT BETWEEN' :
                    lower_bound=np.random.choice(val_bag)
                    upper_bound_bag=[x for x in val_bag if x>=lower_bound]
                    upper_bound=np.random.choice(upper_bound_bag)
                    if var_op=='BETWEEN':
                        term=f" {not_modifier} {tbl_name}.{var_name} BETWEEN {lower_bound} AND {upper_bound} " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} BETWEEN {lower_bound} AND {upper_bound} "
                    else:
                        term=f" {not_modifier} {tbl_name}.{var_name} NOT BETWEEN {lower_bound} AND {upper_bound} " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} NOT BETWEEN {lower_bound} AND {upper_bound} "
                elif var_op=='IN' or var_op=='NOT IN':
                    no_in_terms=random.randint(2,len(val_bag)) if len(val_bag)>2 else 2
                    no_in_terms=min(no_in_terms, self.max_in_terms)
                    vals=np.random.choice(val_bag, size=no_in_terms)
                    for i,x in enumerate(vals): #This will eliminate double quotes in the list of values used in the IN clause
                        try:
                            vals[i]=eval(x)
                        except:
                            continue
                    vals=set(vals)
                    vals_str=f"{vals}"
                    vals_str=vals_str.replace("}",")")
                    vals_str=vals_str.replace("{","(")
                    if var_op=='IN':
                        term =f" {not_modifier} {tbl_name}.{var_name} IN "+vals_str+ " " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} IN "+vals_str+ " "
                    else:
                        term=f" {not_modifier} {tbl_name}.{var_name} NOT IN "+vals_str+ " " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} NOT IN "+vals_str+ " "
                else:
                    val=np.random.choice(val_bag)
                    term=f" {not_modifier} {tbl_name}.{var_name} {var_op} {val} " if len(join_tbl_lst) !=0 else f" {not_modifier} {var_name} {var_op} {val} "
            else:
                raise Exception(f"Can not find {var_name} in the lists of all variables!!")
            
            terms+=term
            if idx <len(picked_vars)-1:
                selected_logic_op=np.random.choice(list(self.ATTRS['LOGIC_OPS'].keys()),  p=list(self.ATTRS['LOGIC_OPS'].values()))
                terms+=selected_logic_op

        return terms
            

    def compile_fltr_expr(self):
        from_expr,from_tbl,join_tbl_lst=self._make_rnd_from_expr()
        where_expr=self._get_rnd_where_expr(from_tbl,join_tbl_lst, drop_fkey=True)
        if len(join_tbl_lst)!=0:
            fltr_type=np.random.choice(list(self.ATTRS['FILTER_TYPE'].keys()), p=list(self.ATTRS['FILTER_TYPE'].values()))
        else:
            fltr_type='WHERE'
        expr='SELECT * '+ from_expr+ f' {fltr_type} ' + where_expr
        return expr,from_tbl,join_tbl_lst


    def make_single_fltr_query(self,single_expr,from_tbl, join_tbl_lst) -> dict:
        dic={}
        # single_expr,from_tbl, join_tbl_lst =self.compile_fltr_expr()
        # print(single_expr) #SMK TMP
        query=self._make_query(self.CUR, single_expr)
        dic['query']=query
        dic['query_desc']={
            "type":"single_fltr",
            "from_tbl_name":from_tbl,
            "join_tbl_name_lst":join_tbl_lst,
            "sql":single_expr,
            "n_rows":query.shape[0],
            "n_cols":query.shape[1]
        }
        return dic



    def make_twin_fltr_query(self,syn_tbl_name_lst,real_expr,real_from_tbl, real_join_tbl_lst ) -> dict:
        self._validate_syn_lst(syn_tbl_name_lst)  #validate syn list
        # real_expr,real_from_tbl, real_join_tbl_lst =self.compile_fltr_expr()
        # print(real_expr) #SMK TMP
        syn_expr=self._expr_replace_tbl_name(real_expr)
        syn_from_tbl=syn_tbl_name_lst[self._get_tbl_index(real_from_tbl)]
        syn_join_tbl_lst=[syn_tbl_name_lst[self._get_tbl_index(real_tbl_name)] for real_tbl_name in real_join_tbl_lst]
        query_real=self._make_query(self.CUR, real_expr)
        query_syn=self._make_query(self.CUR, syn_expr)
        dic={}
        dic['query_real']=query_real
        dic['query_syn']=query_syn
        dic['query_desc']={
            "type":"twin_fltr",
            "from_tbl_name_real":real_from_tbl,
            "join_tbl_name_lst_real":real_join_tbl_lst,
            "sql_real":real_expr,
            "n_cols_real":query_real.shape[1],
            "n_rows_real":query_real.shape[0],
            "from_tbl_name_syn":syn_from_tbl,
            "join_tbl_name_lst_syn":syn_join_tbl_lst,
            "sql_syn":syn_expr,
            "n_cols_syn":query_syn.shape[1],
            "n_rows_syn":query_syn.shape[0],
        }
        return dic


##################################### METHODS FOR GENERATING RANDOM FILTER-AGGREGATE QUERIES #############################

    def compile_aggfltr_expr(self):
        
        from_expr, from_table,join_tbl_lst=self._make_rnd_from_expr() #from table is the table right after the from clause which can be either a sole or parent table  
        groupby_lst=self._get_rnd_groupby_lst(from_table,join_tbl_lst, drop_fkey=True)
        expr2_1=' GROUP BY '
        expr2_2=f'{groupby_lst}'
        expr2_2=expr2_2.replace("[","")
        expr2_2=expr2_2.replace("]","")
        expr2_2=expr2_2.replace("'","")
        
        where_expr=self._get_rnd_where_expr(from_table,join_tbl_lst, drop_fkey=True)
        
        if len(join_tbl_lst)!=0:
            fltr_type=np.random.choice(list(self.ATTRS['FILTER_TYPE'].keys()), p=list(self.ATTRS['FILTER_TYPE'].values()))
        else:
            fltr_type='WHERE'

        log_op,cnt_var=self._get_rnd_agg_fntn_terms(from_table,join_tbl_lst)  
        if log_op != 'None':        
            expr_a=f'SELECT {expr2_2}, COUNT(*), {log_op}({cnt_var}) '+ from_expr
        else:
            expr_a=f'SELECT {expr2_2}, COUNT(*) '+ from_expr
        
        expr_b= f' {fltr_type} ' + where_expr
        expr_c= f' GROUP BY {expr2_2}'
        
        return expr_a+expr_b+expr_c, groupby_lst, from_table, join_tbl_lst, (log_op,cnt_var) 


    def make_single_aggfltr_query(self,single_expr,groupby_lst,from_tbl, join_tbl_lst, agg_fntn_terms) -> dict:
        dic={}
        # single_expr,groupby_lst,from_tbl, join_tbl_lst, agg_fntn_terms=self.compile_aggfltr_expr()
        # print(single_expr) #SMK TEMP
        query=self._make_query(self.CUR, single_expr)
        # grpby_vars=self._drop_tbl_name(groupby_lst)
        dic['query']=query
        dic['query_desc']={
            "type":"single_agg",
            "agg_fntn":agg_fntn_terms,
            "grpby_vars": groupby_lst,
            "from_tbl_name":from_tbl,
            "join_tbl_name_lst":join_tbl_lst,
            "sql":single_expr,
            "n_rows":query.shape[0],
            "n_cols":query.shape[1]
        }
        return dic


    def make_twin_aggfltr_query(self,syn_tbl_name_lst,real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst, agg_fntn_terms) -> dict:
        self._validate_syn_lst(syn_tbl_name_lst)  #validate syn list
        # real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst, agg_fntn_terms=self.compile_aggfltr_expr()
        # print(real_expr) #SMK TEMP

        # if len(real_join_tbl_lst) == 0: #if table is sole
        #     groupby_lst=self._drop_tbl_name(real_groupby_lst)
        # else:
        #     groupby_lst=real_groupby_lst
        syn_expr=self._expr_replace_tbl_name(real_expr)
        syn_from_tbl=syn_tbl_name_lst[self._get_tbl_index(real_from_tbl)]
        
        if len(real_join_tbl_lst) != 0:
            syn_join_tbl_lst=[syn_tbl_name_lst[self._get_tbl_index(real_tbl_name)] for real_tbl_name in real_join_tbl_lst]
        else:
            syn_join_tbl_lst=[]
    
        query_real=self._make_query(self.CUR, real_expr)
        real_col_dic=dict(zip(list(query_real.columns[0:len(real_groupby_lst)]),real_groupby_lst))
        query_real.rename(columns=real_col_dic, inplace=True)
        query_syn=self._make_query(self.CUR, syn_expr)
        syn_col_dic=dict(zip(list(query_syn.columns[0:len(real_groupby_lst)]),real_groupby_lst)) #No need to rename the table names to match these in the synthetic data since matching processes requires that both real and syn tables have same varibale names.
        query_syn.rename(columns=syn_col_dic, inplace=True)

        dic={}

        dic['query_real']=query_real
        dic['query_syn']=query_syn
        dic['query_desc']={
            "type":"twin_agg",
            "agg_fntn":agg_fntn_terms,
            "grpby_vars": real_groupby_lst,
            "from_tbl_name_real":real_from_tbl,
            "join_tbl_name_lst_real":real_join_tbl_lst,
            "sql_real":real_expr,
            "n_cols_real":query_real.shape[1],
            "n_rows_real":query_real.shape[0],
            "from_tbl_name_syn":syn_from_tbl,
            "join_tbl_name_lst_syn":syn_join_tbl_lst,
            "sql_syn":syn_expr,
            "n_cols_syn":query_syn.shape[1],
            "n_rows_syn":query_syn.shape[0],
        }

        return dic


################################ Matching records and calculating metrics methods

    def _match_twin_query(self, rnd_query: dict) ->dict:
        assert 'single' not in rnd_query['query_desc']['type'], "This method does not apply to single random queries!"
        assert '_fltr' not in rnd_query['query_desc']['type'], "This method does not apply to filter random queries. It only applies to aggregate queries!"
        matched_rnd_query={}
        ext_real=rnd_query['query_real']
        ext_syn=rnd_query['query_syn']
        # grpby_vars=rnd_query['query_desc']['grpby_vars'] #vars including real table name
        # real_agg_vars=[x for x in list(query_real.columns) if x not in grpby_vars]
        # syn_agg_vars=[x for x in list(query_syn.columns) if x not in grpby_vars]
        # real=copy.deepcopy(query_real)
        # syn=copy.deepcopy(query_syn)

        #redefine var names into number to assure smooth sorting
        ext_var_names=list(ext_real.columns)
        ext_var_nmbrs=list(range(ext_real.shape[1]))

        ext_real.columns=ext_var_nmbrs
        ext_syn.columns=ext_var_nmbrs
        

        i=2 if rnd_query['query_desc']['agg_fntn'][0] != 'None' else 1
        red_real=ext_real.iloc[:,:-i] #drop count and agg fntn columns
        red_syn=ext_syn.iloc[:,:-i]
        agg_col_names=ext_var_names[-i:]
        agg_col_nmbrs=ext_var_nmbrs[-i:]
        red_real.columns=ext_var_nmbrs[0:-i]
        red_syn.columns=ext_var_nmbrs[0:-i]


        assert (red_real.columns == red_syn.columns).all(),"Real and synthetic queries can not be matched since they have different variable names"


        #in_both=real.merge(syn, how='inner', indicator=False)
        in_real_only=red_real.merge(red_syn,how='outer', indicator=True).loc[lambda x: x['_merge']=='left_only']
        del in_real_only['_merge']
        in_real_only[agg_col_nmbrs]=0
        ext_syn= pd.concat([ext_syn,in_real_only], axis=0, ignore_index=True)
        ext_syn.sort_values(ext_var_nmbrs[0:-i], inplace=True, ignore_index=True)

        in_syn_only=red_real.merge(red_syn,how='outer', indicator=True).loc[lambda x: x['_merge']=='right_only']
        del in_syn_only['_merge']
        in_syn_only[agg_col_nmbrs]=0
        ext_real= pd.concat([ext_real,in_syn_only], axis=0, ignore_index=True) 
        ext_real.sort_values(ext_var_nmbrs[0:-i], inplace=True, ignore_index=True)

        assert len(ext_real)==len(ext_syn)
        ext_real.columns=ext_var_names
        ext_syn.columns=ext_var_names

        matched_rnd_query['query_real']=ext_real
        matched_rnd_query['query_syn']=ext_syn
        matched_rnd_query['query_desc']=rnd_query['query_desc']
        # matched_rnd_query['query_desc']['n_rows_real']=len(ext_real)
        # matched_rnd_query['query_desc']['n_rows_syn']=len(ext_syn)

        return matched_rnd_query



    def calc_dist_scores(self,matched_rnd_query):

        assert 'single' not in matched_rnd_query['query_desc']['type'], "This method does not apply to single random queries!"
        assert '_fltr' not in matched_rnd_query['query_desc']['type'], "This method does not apply to filter random queries. It only applies to aggregate queries!"

        scored_rnd_query=matched_rnd_query
        real=matched_rnd_query['query_real']
        syn=matched_rnd_query['query_syn']
        desc=matched_rnd_query['query_desc']
        cnt_idx=-1 if  desc['agg_fntn'] == 'None' else -2 #decide the column back index of COUNT header
        assert real.iloc[:,:cnt_idx].equals(syn.iloc[:,:cnt_idx]), "Real and Synthetic tables should be matched!"

        if len(real)!=0 and len(syn)!=0: 
            real_probs=real.iloc[:,cnt_idx].astype(float)/sum(real.iloc[:,cnt_idx].astype(float))
            syn_probs=syn.iloc[:,cnt_idx].astype(float)/sum(syn.iloc[:,cnt_idx].astype(float))
            hlngr_dist=np.sqrt(np.sum((np.sqrt(real_probs)-np.sqrt(syn_probs))**2))/np.sqrt(2)
            scored_rnd_query['query_hlngr_score']=hlngr_dist
            
            if cnt_idx==-2:
                pivot=np.concatenate([[real.iloc[:,cnt_idx],syn.iloc[:,cnt_idx],real.iloc[:,-1],syn.iloc[:,-1]]], axis=1).T #count_real, count_syn, agg_real, agg_syn
                pivot=pivot.astype(float)
                pivot=pivot[~np.isnan(pivot).any(axis=1),:]# remove rows with any nan 
                cndn=(pivot[:,0]!=0) & (pivot[:,1]!=0) # dropping rows (ie classes) that do not exist in real or syn queries. Unlike, the Hellinger distance, the Euclidean distance is not meant to measure how good the synthetic model is in terms of teh classes generated.
                pivot=pivot[cndn,:]
                p=pivot[:,-2]
                q=pivot[:,-1]
                scaler = StandardScaler()
                p_q=(p-q).reshape(-1, 1)
                if len(p_q) !=0:
                    pq_s = scaler.fit_transform((p-q).reshape(-1, 1))
                    res=np.linalg.norm(pq_s, 2)/len(pq_s)
                    scored_rnd_query['query_ecldn_score']=res
                else:
                    scored_rnd_query['query_ecldn_score']=np.nan
        else: 
            scored_rnd_query['query_hlngr_score']=np.nan
            scored_rnd_query['query_ecldn_score']=np.nan
         
        return scored_rnd_query

