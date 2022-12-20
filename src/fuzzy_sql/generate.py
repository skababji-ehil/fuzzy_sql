import time
from fuzzy_sql.randomquery import RandomQuery


def gen_aggfltr_queries(n_queries: int, db_path: str, real_tbl_lst: list, metadata_lst: list,  syn_tbl_lst: list, max_query_time=5) -> list:
    ''' The function generates multiple twin random queries of aggregate-filter type. 

    Args:
        n_queries: The required number of queries to be generated.
        db_path: Database full path as string.
        real_tbl_lst: A list of real table names (strings) to be used for generating the random queries. The list may include related tables.
        metadata_list: A list of dictionaries describing the variables and relations for each input table. A single metadata dictionaries is used for each real table and its counterpart synthetic table since both real and synthetic tables shall have identical variables and relations.
        syn_tbl_lst: A list of synthetic table names (strings) to be used for generating the random queries.
        max_query_time: The maximum time in seconds that is allowed to execute a randomly generated query expression before it skips it to the next random expression. 

    Returns: 
        A list of dictionaries where each dictionary includes the query result for real data as a dataframe, the query result for synthetic data as a dataframe, a dictionary describing the query details, a float representing the twin query Hellinger distance and another representing  Euclidean distance, whenever applicable.  
    '''
       
    queries = []
    k = 0
    while k < n_queries:
        start=time.time()
        query_obj = RandomQuery(db_path, real_tbl_lst, metadata_lst)
        real_expr, real_groupby_lst, real_from_tbl, real_join_tbl_lst, agg_fntn_terms = query_obj.compile_aggfltr_expr()
        
        if not query_obj._test_query_time(db_path,real_expr):
            continue
        
        
        rnd_query = query_obj.make_twin_aggfltr_query(syn_tbl_lst, real_expr, real_groupby_lst, real_from_tbl, real_join_tbl_lst, agg_fntn_terms)
        matched_query = query_obj._match_queries4agg(rnd_query)
        scored_query = query_obj.gather_metrics4agg(matched_query)
        queries.append(scored_query)
        k += 1
        end=time.time()
        print('Generated Random Aggregate Filter Query - {} in {:0.1f} seconds.'.format(str(k), end-start))
    
    return queries


def gen_fltr_queries(n_queries: int, db_path: str, real_tbl_lst: list, metadata_lst: list,  syn_tbl_lst: list, max_query_time=5) -> list:
    ''' The function generates multiple twin random queries of filter type. 

    Args:
        n_queries: The required number of queries to be generated.
        db_path: Database full path as string.
        real_tbl_lst: A list of real table names (strings) to be used for generating the random queries. The list may include related tables.
        metadata_list: A list of dictionaries describing the variables and relations for each input table. A single metadata dictionaries is used for each real table and its counterpart synthetic table since both real and synthetic tables shall have identical variables and relations.
        syn_tbl_lst: A list of synthetic table names (strings) to be used for generating the random queries.
        max_query_time: The maximum time in seconds that is allowed to execute a randomly generated query expression before it skips it to the next random expression. 

    Returns: 
        A list of dictionaries where each dictionary includes the query result for real data as a dataframe, the query result for synthetic data as a dataframe, a dictionary describing the query details, a float representing the twin query Hellinger distance and another representing  Euclidean distance, whenever applicable.  
    '''
       
    queries = []
    k = 0
    while k < n_queries:
        start=time.time()
        query_obj = RandomQuery(db_path, real_tbl_lst, metadata_lst)
        real_expr, real_from_tbl, real_join_tbl_lst = query_obj.compile_fltr_expr()
        
        # if not query_obj._test_query_time(db_path,real_expr):
        #     continue
        
        rnd_query = query_obj.make_twin_fltr_query(syn_tbl_lst, real_expr, real_from_tbl, real_join_tbl_lst)
        scored_query=query_obj.gather_metrics4fltr(rnd_query)
        queries.append(scored_query)
        
        k += 1
        end=time.time()
        print('Generated Random Filter Query - {} in {:0.1f} seconds.'.format(str(k), end-start))
        print('\n')
    
    return queries


def calc_tabular_hlngr(db_path: str,  real_table_name:str, metadata: dict, syn_table_name: str):
    ''' The function calculates the Hellinger distance between two tables that are existing in a database. Both tables shall have the same number of observations and variable names. This function does NOT apply to longitudinal data.
    
    Args:
        db_path: A string providing the full path to the database. 
        real_table_name: The name of the table that holds the real data.
        metadata: A dictionary showing the real table name and the type of each of its variables. The metadata dictionary shall conform to Fuzzy SQL schema.
        syn_table_name: The name of the table that holds the synthetic data. The synthetic table in the database shall have the same structure of the real table.
        
    Returns:
        A dictionary with the median, IQR and Standard Deviation of the  Hellinger distance in addition to teh individual Hellinger distance for each variable. 
    '''

    query_obj = RandomQuery(db_path, [real_table_name], [metadata])
    result=query_obj.gather_metrics4tabular(real_table_name,syn_table_name)

    return result