import copy
from http.client import MOVED_PERMANENTLY
import numpy as np
import pandas as pd
import random
from sklearn.preprocessing import StandardScaler


class RND_QUERY():
    """ Generates random queries for tabular and longitudinal datasets. 
    """

    def __init__(self, db_conn: object, tbl_names_lst: list,  metadata_lst: list,params: dict , seed=False):
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

       

        self.SEED=seed
        self.seed_no=141
        
        self.DB_CONN=db_conn
        self.CUR = db_conn.cursor()

    
        ## SMK ## self.SOLE_GRP, self.ROOT_GRP, self.CHILD_GRP, self.PARENT_GRP=self._classify_tables(tbl_names_lst,metadata_lst) 
        self.SOLE_NAME_LST, _, self.CHILD_NAME_LST, self.PARENT_NAME_LST=self._classify_tables(tbl_names_lst,metadata_lst) 

        
        
        # self.PARENT_NAME = parent_tbl_name #RP = Real Parent
        # self.CHILD_NAME = child_tbl_name #RC = Real Child
        # self.metadata=copy.deepcopy(metadata)
        
        #Fetch Real data (both parent and child)
        # self.PARENT_DF=pd.read_sql_query(f'SELECT * FROM {self.PARENT_NAME}', db_conn) #Real Parent Dataframe
        # self.CHILD_DF=pd.read_sql_query(f'SELECT * FROM {self.CHILD_NAME}', db_conn) #Real Child Dataframe


        # #Get foreign key name
        # self.FKEY_NAME=self.metadata['key']

        # #Delete foreign key from child variables to avoid repetition of variable in various expression
        # del self.metadata['child'][self.FKEY_NAME]

        self.TBL_NAME_LST=tbl_names_lst

        #Add to var tuples a valid variable types (ie. CNT, CAT  or DT))
        mod_metadata_lst=[]
        for metadata_i in metadata_lst:
            mod_metadata_lst.append(self._classify_vars(metadata_i))

        self.METADATA_LST=mod_metadata_lst #A list of dictionaries for each table

        # self.CAT_VARS={} #Parent Categorical Variables
        # self.CNT_VARS={}
        # self.DT_VARS={}
        # self.CAT_VARS['parent']=[key for key, value in self.metadata['parent'].items() if value in ['qualitative','categorical','nominal','discrete','ordinal','dichotomous']]
        # self.CAT_VARS['child']=[key for key, value in self.metadata['child'].items() if value in ['qualitative','categorical','nominal','discrete','ordinal','dichotomous']]
        # self.CNT_VARS['parent']=[key for key, value in self.metadata['parent'].items() if value in ['quantitative','continuous','interval','ratio']]
        # self.CNT_VARS['child']=[key for key, value in self.metadata['child'].items() if value in ['quantitative','continuous','interval','ratio']]
        # self.DT_VARS['parent']=[key for key, value in self.metadata['parent'].items() if value in ['date','time','datetime']]
        # self.DT_VARS['child']=[key for key, value in self.metadata['child'].items() if value in ['date','time','datetime']]



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

        self.VAL_LST=[]
        for i, tbl_name in enumerate(self.TBL_NAME_LST):
            val_dict={}
            val_dict['tbl_name']=tbl_name
            for j,var_tpl in enumerate(metadata_lst[i]['var']):
                val_dict[var_tpl[0]]=self._make_val_bag(tbl_name, metadata_lst[i]['var'][j][0])
            self.VAL_LST.append(val_dict)
        
        # # Generate dictionaries of bags for various variables
        # self.CAT_VAL_BAGS={}
        # self.CNT_VAL_BAGS={}
        # self.DT_VAL_BAGS={}
        # self.CAT_VAL_BAGS['parent']=self._make_bags(self.PARENT_DF[self.CAT_VARS['parent']])
        # self.CNT_VAL_BAGS['parent']=self._make_bags(self.PARENT_DF[self.CNT_VARS['parent']])
        # self.DT_VAL_BAGS['parent']=self._make_bags(self.PARENT_DF[self.DT_VARS['parent']])
        # self.CAT_VAL_BAGS['child']=self._make_bags(self.CHILD_DF[self.CAT_VARS['child']])
        # self.CNT_VAL_BAGS['child']=self._make_bags(self.CHILD_DF[self.CNT_VARS['child']])
        # self.DT_VAL_BAGS['child']=self._make_bags(self.CHILD_DF[self.DT_VARS['child']])
        
        self.min_join_clauses=1 #set it larger than one to enforce join clause 
        self.max_no_in_terms=5 #Maximum number of terms for 'in' clause (set it to 0 if you do not want to impose any limit)
    

###################################################################

    def _get_tbl_index(self,tbl_name):
        #lookup table index in METADATA_LST
        for i,metadata in enumerate(self.METADATA_LST):
            if metadata['tbl_name']==tbl_name:
                return i


    def _classify_tables(self,tbl_names_lst,metadata_lst):
        sole_grp=[]
        root_grp=[]
        child_grp=[]
        parent_grp=[]
        for i, tbl in enumerate(tbl_names_lst):
            if metadata_lst[i]['tbl_key_name']=='Null':
                sole_grp.append(tbl)
                continue
            elif metadata_lst[i]['parent_ref']=='Null':
                root_grp.append(tbl)
                continue
            else:
                child_grp.append(tbl)
                parent_tbls=[x[0] for x in metadata_lst[i]['parent_ref']]
                parent_grp.append(parent_tbls)

        parent_grp=list(set([y for x in parent_grp for y in x]))
        return sole_grp,root_grp,child_grp,parent_grp


    def _classify_vars(self,metadata: dict) -> dict:
        mod_metadata=copy.deepcopy(metadata)
        for i, var_tpl in enumerate(mod_metadata['var']):
            if var_tpl[1] in ['quantitative','continuous','interval','ratio', 'REAL']:
                mod_metadata['var'][i].append("CNT")
            elif var_tpl[1] in ['qualitative','categorical','nominal','discrete','ordinal','dichotomous', 'TEXT', 'INTEGER']:
                mod_metadata['var'][i].append("CAT")
            elif var_tpl[1] in ['date','time','datetime']:
                mod_metadata['var'][i].append("DT")
            else:
                raise Exception(f"Variable type {var_tpl[1]} in metadata file is not recognized!")

        return mod_metadata

    def _fetch_var_type(self, table_name: str, var_name:str) -> str:
        # This function fetches the input variable type (CAT, CNT or DT) from the metadata and returns it
        i=self.TBL_NAME_LST.index(table_name) #find table index
        metadata=self.METADATA_LST[i] #get corresponding metadata dictionary for the table
        var_tpl_lst=metadata['var']
        for j,var_tpl in enumerate(var_tpl_lst):#search for variable in metadata tuples and return it type
            if var_tpl[0]==var_name:
                return var_tpl_lst[j][2]


    def _make_val_bag(self,table_name:str, var_name: str) -> list:
        i=self.TBL_NAME_LST.index(table_name)
        df=self.TBL_LST[i]
        vals=df[var_name].values
        vals=[x for x in vals if x==x] #drop nan
        vals=list(filter(None, vals)) #drop None
        var_type=self._fetch_var_type(table_name, var_name)
        if var_type=='CAT':
            #print(table_name,var_name)
            vals=["'"+str(x)+"'" for x in vals if "'" not in x or '"' not in x] # for the values of categorical variables, add quote to avoid errors in SQL statement
        vals=vals if len(vals)!=0 else ['N/A']
        return vals


    def _get_vars_by_type(self,var_type, tbl_name, drop_key):
        # Returns variables by type (i.e CAT, CNT or DT) for the input table by referring to the corresponding metadata
        for i,metadata in enumerate(self.METADATA_LST):#lookup table index in METADATA_LST
            if metadata['tbl_name']==tbl_name:
                tbl_idx=i
                break
        fetched_vars=[]
        var_tpls=self.METADATA_LST[tbl_idx]['var']
        for tpl in var_tpls:
            if tpl[2]==var_type:
                fetched_vars.append(tpl[0])
        if drop_key:
            if self.METADATA_LST[tbl_idx]['tbl_key_name'] in fetched_vars:
                fetched_vars.remove(self.METADATA_LST[tbl_idx]['tbl_key_name'])  
        return fetched_vars

    def _get_tbl_childs(self,tbl_name):
        #search thru all tables and check if tbl_name exists in parenet 
        tbl_childs=[]
        for i,metadata in enumerate(self.METADATA_LST):#lookup table index in METADATA_LST
            if metadata['parent_ref']=='Null':
                continue
            parents=[prnt_tpl[0] for prnt_tpl in metadata['parent_ref']]
            if tbl_name in parents:
                tbl_childs.append(metadata['tbl_name'])
        return list(set(tbl_childs))
    
    def _prepend_tbl_name(self,tbl_name,vars_lst):
        # Returns teh input var names but prepended with the input table
        var_names=[tbl_name+'.'+x for x in vars_lst]
        return var_names

    def _get_join_on_sub_expr(self,tbl1,tbl2):
        #This method deals with both individual and composite keys but it is not tested yet using any composite keys
        tbl1_key_lst=self.METADATA_LST[self._get_tbl_index(tbl1)]['tbl_key_name']
        tbl2_key_lst=self.METADATA_LST[self._get_tbl_index(tbl2)]['tbl_key_name']
        assert len(tbl1_key_lst) !=0, "Joining key is not defined!"
        assert len(tbl1_key_lst)==len(tbl2_key_lst),"Both connected tables shall have the same number of joining keys"
        tbl1_key_lst=self._prepend_tbl_name(tbl1,tbl1_key_lst)
        tbl2_key_lst=self._prepend_tbl_name(tbl2,tbl2_key_lst)
        if len(tbl1_key_lst)==1:
            expr=f" ON {tbl1_key_lst[0]}={tbl2_key_lst[0]} "
        else:
            expr=" ON "
            for i, (tbl1_key, tbl2_key) in enumerate(zip(tbl1_key_lst,tbl2_key_lst)):
                expr+= f"{tbl1_key} = {tbl2_key}"
                if i < len(tbl1_key_lst)-1:
                    expr+=" AND "
        return expr


    def _make_rnd_from_expr(self)-> str:
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)

        if len(self.SOLE_NAME_LST) !=0:
            assert len(self.SOLE_NAME_LST)==1,"For tabular fuzzing, you can not have more than one table passed to the class."
            return f" FROM {self.SOLE_NAME_LST[0]} ", self.SOLE_NAME_LST[0],"Null"
        else:
            from_tbl=random.sample(self.PARENT_NAME_LST,1)[0] # randomly select one parent 
            join_tbls=self._get_tbl_childs(from_tbl)
            picked_no_joins=random.randint(self.min_join_clauses,len(join_tbls)) 
            join_tbl_lst=random.sample(join_tbls,picked_no_joins) #randomly select a number of child tables 
            expr1=f" FROM {from_tbl} "
            expr2=""
            for j in range(picked_no_joins):
                expr2+=f" JOIN {join_tbl_lst[j]} "+self._get_join_on_sub_expr(from_tbl,join_tbl_lst[j])
            return expr1+expr2, from_tbl, join_tbl_lst



    def _get_rnd_groupby_lst(self,from_tbl, join_tbl_lst, drop_fkey=True)-> list:
        #returned randomly picked cat vars including the concatenated table name of the real data (ie that is defined in the class)
        #Note: You can group by CAT_VARS whether from parent or child or both
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)
        all_cat_vars=[]


        if join_tbl_lst != 'Null':
            join_tbl_lst.append(from_tbl)#possible group-by list includes only from_tbl and join_tbl lists
            for tbl_name in join_tbl_lst: #All table types (ie parent and child) can be used in agg queries as long as they have CAT vars
                if drop_fkey:
                    cat_vars=self._get_vars_by_type('CAT',tbl_name,drop_key=True)
                else:
                    cat_vars=self._get_vars_by_type('CAT',tbl_name, drop_key=False)
                cat_vars=self._prepend_tbl_name(tbl_name,cat_vars)
                all_cat_vars.append(cat_vars)
            all_cat_vars=[var for vars in all_cat_vars for var in vars] #flatten

            if len(all_cat_vars)==1:
                raise Exception("The only available categorical variable is the JOIN key. Add more categorical variables or set drop_fkey to False in _get_rnd_groupby_lst")

            # all_cat_vars=self._mix_vars((self.PARENT_NAME,self.CAT_VARS['parent']),(self.CHILD_NAME,self.CAT_VARS['child']))

            # parent_cat_vars=[self.PARENT_NAME+'.'+x for x in self.CAT_VARS['parent']]
            # child_cat_vars=[self.CHILD_NAME+'.'+x for x in self.CAT_VARS['child']]
            # all_cat_vars=parent_cat_vars +child_cat_vars
            n_vars_bag=np.arange(1, 1+len(all_cat_vars)) 
            if self.ATTRS['LESS_GRP_VARS']:#define slope-down discrete distribution 
                n_var_probs=n_vars_bag[::-1]/n_vars_bag.sum()
                # n_var_probs= np.zeros_like(n_vars_bag)
                # n_var_probs[0]=1
                n_vars=np.random.choice(n_vars_bag, p=n_var_probs)
            else:
                n_vars=np.random.choice(n_vars_bag)
            picked_vars = random.sample(all_cat_vars, n_vars)

        else: #If there is only one sole table (ie tabular case)
                if drop_fkey:
                    cat_vars=self._get_vars_by_type('CAT',from_tbl,drop_key=True)
                else:
                    cat_vars=self._get_vars_by_type('CAT',from_tbl, drop_key=False)
                n_vars_bag=np.arange(1, 1+len(cat_vars)) 
                if self.ATTRS['LESS_GRP_VARS']:#define slope-down discrete distribution 
                    n_var_probs=n_vars_bag[::-1]/n_vars_bag.sum()
                    # n_var_probs= np.zeros_like(n_vars_bag)
                    # n_var_probs[0]=1
                    n_vars=np.random.choice(n_vars_bag, p=n_var_probs)
                else:
                    n_vars=np.random.choice(n_vars_bag)
                picked_vars = random.sample(cat_vars, n_vars)

        return picked_vars

    def _get_rnd_agg_fntn_terms(self,from_tbl,join_tbl_lst)-> tuple:
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)
        all_cnt_vars=[]
        if join_tbl_lst != 'Null':
            join_tbl_lst.append(from_tbl)#possible list for agg_fntn operand includes only from_tbl and join_tbl lists
            for tbl_name in join_tbl_lst: 
                cnt_vars=self._get_vars_by_type('CNT',tbl_name,drop_key=False) #drop_key is not used for continuous variable since key is usually categorical variable 
                assert len(cnt_vars)!=0, "No continuous variable is available to use it with an Aggregate Function. Please set agg_fnt to False."
                cnt_vars=self._prepend_tbl_name(tbl_name,cnt_vars)
                all_cnt_vars.append(cnt_vars)
            all_cnt_vars=[var for vars in all_cnt_vars for var in vars] #flatten
            picked_cnt_var = np.random.choice(all_cnt_vars)
        else: #If there is only one sole table (ie tabular case)
            cnt_vars=self._get_vars_by_type('CNT',from_tbl, drop_key=False)
            picked_cnt_var=np.random.choice(cnt_vars)
        picked_log_op=np.random.choice(list(self.ATTRS['AGG_OPS'].keys()), p=list(self.ATTRS['AGG_OPS'].values()))
        return picked_log_op,picked_cnt_var

        




    def _compile_agg_expr(self,agg_fntn) -> str:
        from_expr, from_table,join_tbl_lst=self._make_rnd_from_expr() #from table is the table right after the from clause which can be either a sole or parent table  
        groupby_lst=self._get_rnd_groupby_lst(from_table,join_tbl_lst)
        expr2_1=' GROUP BY '
        expr2_2=f'{groupby_lst}'
        expr2_2=expr2_2.replace("[","")
        expr2_2=expr2_2.replace("]","")
        expr2_2=expr2_2.replace("'","")
        if agg_fntn:
            log_op,cnt_var=self._get_rnd_agg_fntn_terms(from_table,join_tbl_lst)
            expr1=f'SELECT {expr2_2}, COUNT(*), {log_op}({cnt_var}) '+ from_expr
            return expr1+expr2_1+expr2_2, groupby_lst,from_table,join_tbl_lst, (log_op, cnt_var)
        else:
            expr1=f'SELECT {expr2_2}, COUNT(*) '+ from_expr
            return expr1+expr2_1+expr2_2, groupby_lst,from_table,join_tbl_lst, 'Null'
    

    
    def make_query(self,cur: object, query_exp: str)-> pd.DataFrame:
        cur.execute(query_exp)
        query = cur.fetchall()
        query=pd.DataFrame(query, columns=[description[0] for description in cur.description])
        return query


    def make_single_agg_query(self, agg_fntn) -> dict:
        dic={}
        single_expr,groupby_lst,from_tbl, join_tbl_lst, agg_fntn_terms=self._compile_agg_expr(agg_fntn)
        query=self.make_query(self.CUR, single_expr)
        # grpby_vars=self._drop_tbl_name(groupby_lst)
        dic['query']=query
        dic['query_desc']={
            "type":"single_agg",
            "agg_fntn":"None",
            "grpby_vars": groupby_lst,
            "from_tbl_name":from_tbl,
            "join_tbl_name_lst":join_tbl_lst,
            "sql":single_expr,
            "n_rows":query.shape[0],
            "n_cols":query.shape[1]
        }
        return dic

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


    def _expr_replace_tbl_name(self, expr: str)-> str:
        # replaces the table names of the real dataset by the table names of the synthetic datasets.
        real_name_lst=self.TBL_NAME_LST
        syn_name_lst=self.SYN_TBL_NAME_LST
        new_expr=copy.deepcopy(expr)
        for real_name, syn_name in zip(real_name_lst,syn_name_lst):
            new_expr=new_expr.replace(real_name,syn_name)
        return new_expr


    def _drop_tbl_name(self,vars_in: list) ->list:
        vars_out=[]
        for var in vars_in:
            split=var.split(".")
            vars_out.append(split[1])
        return vars_out


    def make_twin_agg_query(self, syn_tbl_name_lst, agg_fntn):
        self._validate_syn_lst(syn_tbl_name_lst)  #validate syn list
        real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst,agg_fntn_terms=self._compile_agg_expr(agg_fntn)
        if real_join_tbl_lst != 'Null':
            groupby_lst=self._drop_tbl_name(real_groupby_lst)
        else:
            groupby_lst=real_groupby_lst
        syn_expr=self._expr_replace_tbl_name(real_expr)
        syn_from_tbl=syn_tbl_name_lst[self._get_tbl_index(real_from_tbl)]
        
        if real_join_tbl_lst != 'Null':
            syn_join_tbl_lst=[syn_tbl_name_lst[self._get_tbl_index(real_tbl_name)] for real_tbl_name in real_join_tbl_lst]
        else:
            syn_join_tbl_lst='Null'
        dic={}
        query_real=self.make_query(self.CUR, real_expr)
        query_syn=self.make_query(self.CUR, syn_expr)
        #grpby_vars=self._drop_tbl_name(real_grp_lst)
        dic['query_real']=query_real
        dic['query_syn']=query_syn
        dic['query_desc']={
            "type":"twin_agg",
            "agg_fntn":"None",
            "grpby_vars": groupby_lst,
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

#################  PREVIOUS METHODS BELOW THIS LINE #############################
############################################################################################
    # def _make_bags(self,df:pd.DataFrame)-> dict:
    #     val_bags={}
    #     for var in df.columns:
    #         vals=df[var].values
    #         vals=[x for x in vals if x==x] #drop nan
    #         vals=list(filter(None, vals)) #drop None
    #         if var in self.CAT_VARS['parent']+self.CAT_VARS['child']:
    #             vals=["'"+str(x)+"'" for x in vals if "'" not in x or '"' not in x] # for the values of categorical variables, add quote to avoid errors in SQL statement
    #         val_bags[var]=vals if len(vals)!=0 else ['N/A']
    #     return val_bags



    # def _get_var_idx(self, var_name):
    #     if var_name in self.PARENT_DF.columns: #search in parent
    #         idx=self.PARENT_DF.columns.get_loc(var_name)
    #         return 'parent',idx
    #     elif var_name in self.CHILD_DF.columns: #search in child
    #         idx=self.CHILD_DF.columns.get_loc(var_name)
    #         return 'child', idx
    #     else:
    #         raise Exception(f"{var_name} not found in parent or child tables!")

    # def _mix_vars(self,*args):
    #     #accepts variable length of arguments as tuples where each tuple consists of the table name and some variables that belong to that table
    #     # returns mixed variables in one list but each variable is concatenated with its respective table name
    #     mixed_vars=[]
    #     for arg in args:
    #         vars=[arg[0]+'.'+x for x in arg[1]]
    #         mixed_vars+=vars
    #     return mixed_vars

    # def _change_tbl_name(self, in_lst: list,in_parent_name: str, out_parent_name: str, in_child_name: str, out_child_name: str)-> list:
    #     # replaces the table names of the real dataset by the table names of the synthetic datasets.
    #     out_lst=[var.replace(in_parent_name,out_parent_name ) for var in in_lst] 
    #     out_lst=[var.replace(in_child_name,out_child_name ) for var in out_lst] 
    #     return out_lst

    # def _drop_tbl_name(self,vars_in: list) ->list:
    #     vars_out=[]
    #     for var in vars_in:
    #         split=var.split(".")
    #         vars_out.append(split[1])
    #     return vars_out




    def _get_rnd_aggfntn_tpl(self) -> tuple:
        #returns a random tuple of agg function and continuous OR date variable 
        # Note: continuous variable can be from either parent or child tables 
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)
        all_possible_vars=self._mix_vars((self.PARENT_NAME,self.CNT_VARS['parent']),(self.PARENT_NAME,self.DT_VARS['parent']),(self.CHILD_NAME,self.CNT_VARS['child']),(self.CHILD_NAME,self.DT_VARS['child']))
        picked_var=np.random.choice(all_possible_vars)
        picked_op=np.random.choice(list(self.ATTRS['AGG_OPS'].keys()), p=list(self.ATTRS['AGG_OPS'].values()))
        return (picked_op,picked_var)

    
    def _get_rnd_where_lst(self, drop_fkey=True) -> tuple:
        # use WHERE with mix of CAT, CNT, DT variables from both PARENT and CHILD
        if self.SEED:
            np.random.seed(self.seed_no)
            random.seed(self.seed_no)
        all_possible_vars=self._mix_vars((self.PARENT_NAME,self.CAT_VARS['parent']),(self.CHILD_NAME,self.CAT_VARS['child']),(self.PARENT_NAME,self.CNT_VARS['parent']),(self.CHILD_NAME,self.CNT_VARS['child']),(self.PARENT_NAME,self.DT_VARS['parent']),(self.CHILD_NAME,self.DT_VARS['child']))
        
        if drop_fkey:
            all_possible_vars=[x for x in  all_possible_vars if  self.FKEY_NAME not in  x]
            if len(all_possible_vars)==1:
                raise Exception("Only join key is available as a variable!! Add more variables.")

        n_vars_bag=np.arange(1, 1+len(all_possible_vars)) #this gives possible number of terms in the where clause
        if self.ATTRS['LESS_CMP_VARS']:#define slope-down discrete distribution 
            n_var_probs=n_vars_bag[::-1]/n_vars_bag.sum()
            # n_var_probs= np.zeros_like(n_vars_bag)
            # n_var_probs[0]=1
            n_vars=np.random.choice(n_vars_bag, p=n_var_probs)
        else:
            n_vars=np.random.choice(n_vars_bag)
        picked_vars = random.sample(all_possible_vars, n_vars)

        all_cat_vars=np.concatenate(list(self.CAT_VARS.values()))
        all_cnt_vars=np.concatenate(list(self.CNT_VARS.values()))
        all_dt_vars=np.concatenate(list(self.DT_VARS.values()))
        terms=[]
        log_ops=[]
        for long_var_name in picked_vars: #This loop will find the a proper random value comparison operation and proper random value for all the picked variables
            #var=long_var_name[long_var_name.find(".")+1:]
            x=long_var_name.split(".") #Note: it is assumed that variable names do NOT include any "."
            var_tbl=x[0]
            var=x[1]
            var_tbl_rank='parent' if var_tbl==self.PARENT_NAME else 'child'
            
            #adding not to long variable name 
            not_status=np.random.choice(list(self.ATTRS['NOT_STATE'].keys()), p=list(self.ATTRS['NOT_STATE'].values()) )
            selected_long_var_name= 'NOT '+long_var_name if not_status=='1' else long_var_name
            
            if var in all_cat_vars:
                picked_cmp_op=np.random.choice(list(self.ATTRS['CAT_OPS'].keys()),p=list(self.ATTRS['CAT_OPS'].values()))
                if picked_cmp_op=='IN' or picked_cmp_op=='NOT IN' :
                    possible_no_of_in_terms=np.arange(2,len(self.CAT_VAL_BAGS[var_tbl_rank][var]))
                    no_of_in_terms=np.min([np.random.choice(possible_no_of_in_terms),self.max_no_in_terms]) if self.max_no_in_terms != 0 else np.random.choice(possible_no_of_in_terms)
                    vals=np.random.choice(self.CAT_VAL_BAGS[var_tbl_rank][var], size=no_of_in_terms)
                    for i,x in enumerate(vals): #This will eliminate double quotes in the list of values used in th eIN clause
                        try:
                            vals[i]=eval(x)
                        except:
                            continue
                    if picked_cmp_op=='IN':
                        term =f" {selected_long_var_name} IN {tuple(vals)} "
                    else:
                        term=f" {selected_long_var_name} NOT IN {tuple(vals)} "
                else:
                    val=np.random.choice(self.CAT_VAL_BAGS[var_tbl_rank][var])
                    term=f" {selected_long_var_name} {picked_cmp_op} {val} "
            
            elif var in all_cnt_vars:
                picked_cmp_op=np.random.choice(list(self.ATTRS['CNT_OPS'].keys()),p=list(self.ATTRS['CNT_OPS'].values()))
                if picked_cmp_op=='BETWEEN' or picked_cmp_op=='NOT BETWEEN':
                    lower_bound_bag=self.CNT_VAL_BAGS[var_tbl_rank][var]
                    lower_bound=np.random.choice(lower_bound_bag)
                    upper_bound_bag=[x for x in lower_bound_bag if x>=lower_bound]
                    upper_bound=np.random.choice(upper_bound_bag)
                    if picked_cmp_op=='BETWEEN':
                        term=f" {selected_long_var_name} BETWEEN {lower_bound} AND {upper_bound} "
                    else:
                        term=f" {selected_long_var_name} NOT BETWEEN {lower_bound} AND {upper_bound} "
                else:
                    val=np.random.choice(self.CNT_VAL_BAGS[var_tbl_rank][var])
                    term=f" {selected_long_var_name} {picked_cmp_op} {val} "
            
            elif var in all_dt_vars:
                picked_cmp_op=np.random.choice(list(self.ATTRS['DT_OPS'].keys()),p=list(self.ATTRS['DT_OPS'].values()))
                if picked_cmp_op=='BETWEEN':
                    pass #To complete
                elif picked_cmp_op=='IN':
                    pass #To complete
                else:
                    pass#To complete
            else:
                raise Exception(f"Can not find {var} in the lists of all variables!!")
            
            terms.append(term)
        
        selected_logic_ops=np.random.choice(list(self.ATTRS['LOGIC_OPS'].keys()), size=len(terms)-1, p=list(self.ATTRS['LOGIC_OPS'].values()))
        return terms, selected_logic_ops

    # if len(real.index)<len(syn.index):
    #     raise Exception("It is likely that a continuous variable is mistakenly defined as nominal in metadata ")
    def match_twin_query(self, rnd_query: dict) ->dict:
        assert 'single' not in rnd_query['query_desc']['type'], "This method does not apply to single random queries!"
        assert '_fltr' not in rnd_query['query_desc']['type'], "This method does not apply to filter random queries. It only applies to aggregate queries!"
        matched_rnd_query={}
        query_real=rnd_query['query_real']
        query_syn=rnd_query['query_syn']
        grpby_vars=rnd_query['query_desc']['grpby_vars']
        real_agg_vars=[x for x in list(query_real.columns) if x not in grpby_vars]
        syn_agg_vars=[x for x in list(query_syn.columns) if x not in grpby_vars]
        real=query_real[grpby_vars]
        syn=query_syn[grpby_vars]

        #in_both=real.merge(syn, how='inner', indicator=False)
        in_real_only=real.merge(syn,how='outer', indicator=True).loc[lambda x: x['_merge']=='left_only']
        del in_real_only['_merge']
        in_real_only[syn_agg_vars]=0
        ext_syn= pd.concat([query_syn,in_real_only], axis=0, ignore_index=True)#extended synthetic
        ext_syn.sort_values(grpby_vars, inplace=True, ignore_index=True)

        in_syn_only=real.merge(syn,how='outer', indicator=True).loc[lambda x: x['_merge']=='right_only']
        del in_syn_only['_merge']
        in_syn_only[real_agg_vars]=0
        ext_real= pd.concat([query_real,in_syn_only], axis=0, ignore_index=True) #extended real
        ext_real.sort_values(grpby_vars, inplace=True, ignore_index=True)

        assert len(ext_real)==len(ext_syn)
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
        cnt_idx=-1 if  desc['aggfntn'] == 'None' else -2 #decide the column back index of COUNT header
        assert real.iloc[:,:cnt_idx].equals(syn.iloc[:,:cnt_idx]), "Real and Synthetic tables should be matched!"

        if len(real)!=0 and len(syn)!=0: 
            real_probs=real.iloc[:,-1]/sum(real.iloc[:,-1])
            syn_probs=syn.iloc[:,-1]/sum(syn.iloc[:,-1])
            hlngr_dist=np.sqrt(np.sum((np.sqrt(real_probs)-np.sqrt(syn_probs))**2))/np.sqrt(2)
            scored_rnd_query['query_hlngr_score']=hlngr_dist
        else: 
            scored_rnd_query['query_hlngr_score']=np.nan

        if cnt_idx==-2:
            pivot=np.concatenate([[real.iloc[:,-2],syn.iloc[:,-2],real.iloc[:,-1],syn.iloc[:,-1]]], axis=1).T
            cndn=(pivot[:,0]!=0) & (pivot[:,1]!=0)  # dropping rows (ie classes) that do not exist in real or syn queries. Unlike, the Hellinger distance, the Euclidean distance is not meant to measure how good the synthetic model is in terms of teh classes generated.
            pivot=pivot[cndn]
            p=pivot[:,-2]
            q=pivot[:,-1]
            scaler = StandardScaler()
            p_q=(p-q).reshape(-1, 1)
            if len(p_q) !=0:
                pq_s = scaler.fit_transform((p-q).reshape(-1, 1))
                res=np.linalg.norm(pq_s, 2)/len(pq_s)
            else:
                res=np.nan
            
            scored_rnd_query['query_ecldn_score']=res

        return scored_rnd_query

    
    def calc_mltpl_dist_scores(self,unmatched_queries: dict)-> dict:
        scored_queries=[]
        for twin in unmatched_queries:
            matched_twin=self.match_twin_query(twin)
            scored_twin=self.calc_dist_scores(matched_twin)
            scored_queries.append(scored_twin)
        return scored_queries

#########################################################################





    def make_mltpl_twin_agg_query(self, n_queries, twin_parent_name, twin_child_name ):
        queries = []
        for k in range(n_queries):
            queries.append(self.make_twin_agg_query(twin_parent_name,twin_child_name))
            print('Generated Random Aggregate Query - {} '.format(str(k)))
        print('\n')
        return queries

#-------------------------------------------------------------------------------------------

    def _build_agg_expr_w_aggfntn(self,pname: str, cname: str, fkey: str, agg_fntn_tpl: tuple, groupby_lst: list) -> str:
        expr2_1=' GROUP BY '
        expr2_2=f'{groupby_lst}'
        expr2_2=expr2_2.replace("[","")
        expr2_2=expr2_2.replace("]","")
        expr2_2=expr2_2.replace("'","")
        expr1=f'SELECT {expr2_2}, COUNT(*), {agg_fntn_tpl[0]}({agg_fntn_tpl[1]}) FROM {pname} JOIN {cname} ON {pname}.{fkey} = {cname}.{fkey}'
        expr=expr1+expr2_1+expr2_2
        return expr
    

    def make_single_agg_query_w_aggfntn(self):
        dic={}
        single_grp_lst=self._get_rnd_groupby_lst()
        agg_fntn_tpl=self._get_rnd_aggfntn_tpl()
        expr=self._build_agg_expr_w_aggfntn(self.PARENT_NAME,self.CHILD_NAME, self.FKEY_NAME,agg_fntn_tpl,single_grp_lst)
        query=self.make_query(self.CUR, expr)
        grpby_vars=self._drop_tbl_name(single_grp_lst)
        dic['query']=query
        dic['query_desc']={
            "type":"single_agg",
            "aggfntn":f"{agg_fntn_tpl[0]}({agg_fntn_tpl[1]})",
            "grpby_vars": grpby_vars,
            "parent_name":self.PARENT_NAME,
            "child_name":self.CHILD_NAME,
            "sql":expr,
            "n_rows":query.shape[0],
            "n_cols":query.shape[1]
        }
        return dic

    def make_twin_agg_query_w_aggfntn(self,twintbl_parent_name,twintbl_child_name):
        dic={}
        real_groupby_lst=self._get_rnd_groupby_lst()
        syn_groupby_lst=self._expr_replace_tbl_name(real_groupby_lst, self.PARENT_NAME,twintbl_parent_name, self.CHILD_NAME, twintbl_child_name)
        real_aggfntn_tpl=self._get_rnd_aggfntn_tpl()
        syn_aggfntn_tpl=self._expr_replace_tbl_name(real_aggfntn_tpl, self.PARENT_NAME,twintbl_parent_name, self.CHILD_NAME, twintbl_child_name)
        real_expr=self._build_agg_expr_w_aggfntn(self.PARENT_NAME,self.CHILD_NAME,self.FKEY_NAME,real_aggfntn_tpl, real_groupby_lst)
        syn_expr=self._build_agg_expr_w_aggfntn(twintbl_parent_name,twintbl_child_name,self.FKEY_NAME,syn_aggfntn_tpl, syn_groupby_lst)
        query_real=self.make_query(self.CUR, real_expr)
        query_syn=self.make_query(self.CUR, syn_expr)

        grpby_vars=self._drop_tbl_name(real_groupby_lst)

        dic['query_real']=query_real
        dic['query_syn']=query_syn
        dic['query_desc']={
            "type":"twin_agg",
            "aggfntn":f"{real_aggfntn_tpl[0]}({real_aggfntn_tpl[1]})",
            "grpby_vars": grpby_vars,
            "parent_name_real":self.PARENT_NAME,
            "child_name_real":self.CHILD_NAME,
            "sql_real":real_expr,
            "n_cols_real":query_real.shape[1],
            "n_rows_real":query_real.shape[0],
            "parent_name_syn":twintbl_parent_name,
            "child_name_syn":twintbl_child_name,
            "sql_syn":syn_expr,
            "n_cols_syn":query_syn.shape[1],
            "n_rows_syn":query_syn.shape[0],
            }
        return dic

    def make_mltpl_twin_agg_query_w_aggfntn(self, n_queries, twin_parent_name, twin_child_name ):
        queries = []
        for k in range(n_queries):
            queries.append(self.make_twin_agg_query_w_aggfntn(twin_parent_name,twin_child_name))
            print('Generated Random Aggregate Query with a Function - {} '.format(str(k)))
        print('\n')
        return queries

#######################################################################################

    def _build_fltr_expr(self,  pname: str, cname: str, fkey: str, where_terms: list, log_ops:list ) -> str:
        expr1=f'SELECT * FROM {pname} JOIN {cname} ON {pname}.{fkey} = {cname}.{fkey} WHERE '
        where_expr=[None]*(len(where_terms)+len(log_ops))
        where_expr[::2]=where_terms
        where_expr[1::2]=log_ops
        where_expr=' '.join(x for x in where_expr )
        where_expr=where_expr + ' '
        return expr1+where_expr


    def make_single_fltr_query(self) -> dict:
        dic={}
        where_terms, log_ops=self._get_rnd_where_lst()
        single_expr=self._build_fltr_expr(self.PARENT_NAME,self.CHILD_NAME, self.FKEY_NAME, where_terms, log_ops)
        query=self.make_query(self.CUR, single_expr)
        dic['query']=query
        dic['query_desc']={
            "type":"single_fltr",
            "aggfntn":"None",
            "parent_name":self.PARENT_NAME,
            "child_name":self.CHILD_NAME,
            "sql":single_expr,
            "n_rows":query.shape[0],
            "n_cols":query.shape[1]
        }
        return dic

    def make_twin_fltr_query(self, twin_parent_name: str, twin_child_name:str) -> dict:
        dic={}
        real_where_terms, log_ops =self._get_rnd_where_lst()
        syn_where_terms=self._expr_replace_tbl_name(real_where_terms, self.PARENT_NAME,twin_parent_name, self.CHILD_NAME,twin_child_name)
        real_expr=self._build_fltr_expr(self.PARENT_NAME, self.CHILD_NAME,self.FKEY_NAME, real_where_terms, log_ops)
        syn_expr=self._build_fltr_expr(twin_parent_name, twin_child_name,self.FKEY_NAME, syn_where_terms,log_ops)
        query_real=self.make_query(self.CUR, real_expr)
        query_syn=self.make_query(self.CUR, syn_expr)

        
        dic['query_real']=query_real
        dic['query_syn']=query_syn
        dic['query_desc']={
            "type":"twin_fltr",
            "aggfntn":"None",
            "parent_name_real":self.PARENT_NAME,
            "child_name_real":self.CHILD_NAME,
            "sql_real":real_expr,
            "n_cols_real":query_real.shape[1],
            "n_rows_real":query_real.shape[0],
            "parent_name_syn":twin_parent_name,
            "child_name_syn":twin_child_name,
            "sql_syn":syn_expr,
            "n_cols_syn":query_syn.shape[1],
            "n_rows_syn":query_syn.shape[0],
        }
        return dic

    def make_mltpl_twin_fltr_query(self, n_queries, twin_parent_name, twin_child_name ):
        queries = []
        for k in range(n_queries):
            queries.append(self.make_twin_fltr_query(twin_parent_name,twin_child_name))
            print('Generated Random Filter Query - {} '.format(str(k)))
        print('\n')
        return queries

##########################################################################################

    def _build_aggfltr_expr(self,  pname: str, cname: str, fkey: str, groupby_lst: list, where_terms: list, log_ops: list) -> str:
        expr2=np.random.choice(list(self.ATTRS['JOIN_CNDTN'].keys()), p=list(self.ATTRS['JOIN_CNDTN'].values()))+' '
        expr2_1=[None]*(len(where_terms)+len(log_ops))
        expr2_1[::2]=where_terms
        expr2_1[1::2]=log_ops
        expr2_1=' '.join(x for x in expr2_1)
        expr2_1='('+expr2_1+')' 
        expr3_1=' GROUP BY '
        expr3_2=f'{groupby_lst}'
        expr3_2=expr3_2.replace("[","")
        expr3_2=expr3_2.replace("]","")
        expr3_2=expr3_2.replace("'","")
        expr1=f'SELECT {expr3_2} ,COUNT(*) FROM {pname} JOIN {cname} ON {pname}.{fkey} = {cname}.{fkey} '

        return expr1+expr2+expr2_1+expr3_1+expr3_2


    def make_single_aggfltr_query(self) -> dict:
        dic={}
        grp_lst=self._get_rnd_groupby_lst()
        where_terms, log_ops=self._get_rnd_where_lst()
        single_expr=self._build_aggfltr_expr(self.PARENT_NAME, self.CHILD_NAME, self.FKEY_NAME,grp_lst, where_terms,log_ops )
        query=self.make_query(self.CUR, single_expr)
        grpby_vars=self._drop_tbl_name(grp_lst)
        dic['query']=query
        dic['query_desc']={
            "type":"single_aggfltr",
            "aggfntn":"None",
            "grpby_vars": grpby_vars,
            "parent_name":self.PARENT_NAME,
            "child_name":self.CHILD_NAME,
            "sql":single_expr,
            "n_rows":query.shape[0],
            "n_cols":query.shape[1]
        }
        return dic


    def make_twin_aggfltr_query(self, twin_parent_name: str, twin_child_name:str) -> dict:
        dic={}
        
        real_grp_lst =self._get_rnd_groupby_lst()
        syn_grp_lst=self._expr_replace_tbl_name(real_grp_lst, self.PARENT_NAME,twin_parent_name, self.CHILD_NAME,twin_child_name)

        real_where_terms, log_ops =self._get_rnd_where_lst()
        syn_where_terms=self._expr_replace_tbl_name(real_where_terms, self.PARENT_NAME,twin_parent_name, self.CHILD_NAME,twin_child_name)
        
        real_expr=self._build_aggfltr_expr(self.PARENT_NAME, self.CHILD_NAME, self.FKEY_NAME,real_grp_lst, real_where_terms,log_ops)
        print(real_expr+'\n')
        syn_expr=self._build_aggfltr_expr(twin_parent_name, twin_child_name, self.FKEY_NAME,syn_grp_lst, syn_where_terms,log_ops)

        query_real=self.make_query(self.CUR, real_expr)
        query_syn=self.make_query(self.CUR, syn_expr)

        grpby_vars=self._drop_tbl_name(real_grp_lst)
        dic['query_real']=query_real
        dic['query_syn']=query_syn
        dic['query_desc']={
            "type":"twin_aggfltr",
            "aggfntn":"None",
            "grpby_vars": grpby_vars,
            "parent_name_real":self.PARENT_NAME,
            "child_name_real":self.CHILD_NAME,
            "sql_real":real_expr,
            "n_cols_real":query_real.shape[1],
            "n_rows_real":query_real.shape[0],
            "parent_name_syn":twin_parent_name,
            "child_name_syn":twin_child_name,
            "sql_syn":syn_expr,
            "n_cols_syn":query_syn.shape[1],
            "n_rows_syn":query_syn.shape[0],
        }
        return dic


    def make_mltpl_twin_aggfltr_query(self, n_queries, twin_parent_name, twin_child_name ):
        queries = []
        for k in range(n_queries):
            queries.append(self.make_twin_aggfltr_query(twin_parent_name,twin_child_name))
            print('Generated Random Aggregate Filter Query - {} '.format(str(k)))
        print('\n')
        return queries
#-----------------------------------------------------------------------------------------------------

    def _build_aggfltr_expr_w_aggfntn(self,pname: str, cname: str, fkey: str, agg_fntn_tpl: tuple, groupby_lst: list, where_terms: list, log_ops: list) -> str:
        expr2=np.random.choice(list(self.ATTRS['JOIN_CNDTN'].keys()), p=list(self.ATTRS['JOIN_CNDTN'].values()))+' '
        expr2_1=[None]*(len(where_terms)+len(log_ops))
        expr2_1[::2]=where_terms
        expr2_1[1::2]=log_ops
        expr2_1=' '.join(x for x in expr2_1)
        expr2_1='('+expr2_1+')' 
        expr3_1=' GROUP BY '
        expr3_2=f'{groupby_lst}'
        expr3_2=expr3_2.replace("[","")
        expr3_2=expr3_2.replace("]","")
        expr3_2=expr3_2.replace("'","")
        expr1=f'SELECT {expr3_2}, COUNT(*), {agg_fntn_tpl[0]}({agg_fntn_tpl[1]}) FROM {pname} JOIN {cname} ON {pname}.{fkey} = {cname}.{fkey} '

        return expr1+expr2+expr2_1+expr3_1+expr3_2



    def make_single_aggfltr_query_w_aggfntn(self) -> dict:
        dic={}
        agg_fntn_tpl=self._get_rnd_aggfntn_tpl()
        grp_lst=self._get_rnd_groupby_lst()
        where_terms, log_ops=self._get_rnd_where_lst()
        single_expr=self._build_aggfltr_expr_w_aggfntn(self.PARENT_NAME, self.CHILD_NAME, self.FKEY_NAME,agg_fntn_tpl,grp_lst, where_terms,log_ops )
        query=self.make_query(self.CUR, single_expr)
        grpby_vars=self._drop_tbl_name(grp_lst)
        dic['query']=query
        dic['query_desc']={
            "type":"single_aggfltr",
            "aggfntn":f"{agg_fntn_tpl[0]}({agg_fntn_tpl[1]})",
            "grpby_vars": grpby_vars,
            "parent_name":self.PARENT_NAME,
            "child_name":self.CHILD_NAME,
            "sql":single_expr,
            "n_rows":query.shape[0],
            "n_cols":query.shape[1]
        }
        return dic



    def make_twin_aggfltr_query_w_aggfntn(self, twin_parent_name: str, twin_child_name:str) -> dict:
        dic={}
        real_agg_fntn_tpl=self._get_rnd_aggfntn_tpl()
        syn_agg_fntn_tpl=tuple(self._expr_replace_tbl_name(real_agg_fntn_tpl, self.PARENT_NAME,twin_parent_name, self.CHILD_NAME,twin_child_name))

        real_grp_lst =self._get_rnd_groupby_lst()
        syn_grp_lst=self._expr_replace_tbl_name(real_grp_lst, self.PARENT_NAME,twin_parent_name, self.CHILD_NAME,twin_child_name)

        real_where_terms, log_ops =self._get_rnd_where_lst()
        syn_where_terms=self._expr_replace_tbl_name(real_where_terms, self.PARENT_NAME,twin_parent_name, self.CHILD_NAME,twin_child_name)

        real_expr=self._build_aggfltr_expr_w_aggfntn(self.PARENT_NAME, self.CHILD_NAME, self.FKEY_NAME,real_agg_fntn_tpl,real_grp_lst, real_where_terms,log_ops )
        syn_expr=self._build_aggfltr_expr_w_aggfntn(twin_parent_name, twin_child_name, self.FKEY_NAME,syn_agg_fntn_tpl,syn_grp_lst, syn_where_terms,log_ops )

        query_real=self.make_query(self.CUR, real_expr)
        query_syn=self.make_query(self.CUR, syn_expr)

        grpby_vars=self._drop_tbl_name(real_grp_lst)

        dic['query_real']=query_real
        dic['query_syn']=query_syn
        dic['query_desc']={
            "type":"twin_aggfltr",
            "aggfntn":f"{real_agg_fntn_tpl[0]}({real_agg_fntn_tpl[1]})",
            "grpby_vars": grpby_vars,
            "parent_name_real":self.PARENT_NAME,
            "child_name_real":self.CHILD_NAME,
            "sql_real":real_expr,
            "n_cols_real":query_real.shape[1],
            "n_rows_real":query_real.shape[0],
            "parent_name_syn":twin_parent_name,
            "child_name_syn":twin_child_name,
            "sql_syn":syn_expr,
            "n_cols_syn":query_syn.shape[1],
            "n_rows_syn":query_syn.shape[0],
        }
        return dic

    def make_mltpl_twin_aggfltr_query_w_aggfntn(self, n_queries, twin_parent_name, twin_child_name ):
        queries = []
        for k in range(n_queries):
            queries.append(self.make_twin_aggfltr_query_w_aggfntn(twin_parent_name,twin_child_name))
            print('Generated Random Aggregate Filter Query with Function - {} '.format(str(k)))
        print('\n')
        return queries
