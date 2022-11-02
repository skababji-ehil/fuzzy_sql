import sys
sys.path.append('/home/samer/projects/fuzzy_sql/src') #This will enable reading the modules
from pathlib import Path
import os
import json
import multiprocessing
import time

from fuzzy_sql.rnd_query import RND_QUERY
from fuzzy_sql.fuzzy_sql import *



if __name__== "__main__":
    #set directories
    root_dir=Path('/home/samer/projects/fuzzy_sql')
    metadata_dir=os.path.join(root_dir,'data/sdgd/processed/metadata')
    db_path=os.path.join(root_dir,'db/sdgd.db')

    #define input tables and metadata
    tbl_names_lst=["C1"]
    syn_tbl_name_lst=['C1_syn_default_1']

    metadata_lst=[]
    for tbl_name in tbl_names_lst:
        with open(os.path.join(metadata_dir,tbl_name+'.json'),'r') as f:
            metadata_lst.append(json.load(f))

    #connect to db
    conn = sqlite3.connect(db_path)


    n_queries=10
    queries = []
    timeout=0.01 #maximum time in secs to execute the query, otherwise it will be skipped to next random query 
    for k in range(n_queries):
        query_obj=RND_QUERY(conn, tbl_names_lst, metadata_lst)
        query_obj.no_groupby_vars=2
        query_obj.no_where_vars=3
        real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst,agg_fntn_terms=query_obj.compile_agg_expr()
        p = multiprocessing.Process(target=query_obj._test_query_time, name="_test_query_time", args=(real_expr,))
        p.start()
        p.join(5) #wait 5 seconds until process terminates
        if p.is_alive():
            p.terminate()
            p.join()
            print('Cant wait any further! I am skipping to next random query!')
            continue
        rnd_query=query_obj.make_twin_agg_query(syn_tbl_name_lst,real_expr,real_groupby_lst,real_from_tbl, real_join_tbl_lst,agg_fntn_terms)
        matched_query=query_obj._match_twin_query(rnd_query)
        scored_query=query_obj.calc_dist_scores(matched_query)
        queries.append(scored_query)
        print('Generated Random Aggregate Filter Query - {} '.format(str(k)))
   






    

  



