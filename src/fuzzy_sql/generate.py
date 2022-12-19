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
    
    return queries



