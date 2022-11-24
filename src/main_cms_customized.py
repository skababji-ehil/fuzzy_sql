import matplotlib
matplotlib.use('Agg')

import sys
sys.path.append('/home/samer/projects/fuzzy_sql/src')

from fuzzy_sql.fuzzy_sql import *
import json
import os
from pathlib import Path




if __name__ == "__main__":
    # set directories
    root_dir = Path('/home/samer/projects/fuzzy_sql')
    metadata_dir = os.path.join(root_dir, 'data/cms/processed/metadata')
    db_path = os.path.join(root_dir, 'db/cms.db')

    # define input tables and metadata
    real_tbl_lst=['s1_ben_sum_2008','s1_ben_sum_2009','s1_ben_sum_2010','s1_carrier_1a','s1_carrier_1b','s1_inpatient','s1_outpatient','s1_prescrp']
    syn_tbl_lst=['s2_ben_sum_2008','s2_ben_sum_2009','s2_ben_sum_2010','s2_carrier_1a','s2_carrier_1b','s2_inpatient','s2_outpatient','s2_prescrp']

    metadata_lst = []
    for tbl_name in real_tbl_lst:
        with open(os.path.join(metadata_dir, tbl_name+'.json'), 'r') as f:
            metadata_lst.append(json.load(f))

    # connect to db
    conn = sqlite3.connect(db_path)

    # Generate queries with possibility to customize class attributes 
    n_queries=10
    queries = []
    k = 0
    while k < n_queries:
        query_obj = RndQry(conn, real_tbl_lst, metadata_lst)
        query_obj.no_groupby_vars = 2
        query_obj.no_where_vars = 2
        query_obj.no_join_tables = 2
        real_expr, real_groupby_lst, real_from_tbl, real_join_tbl_lst, agg_fntn_terms = query_obj.compile_aggfltr_expr()
        if not query_obj._test_query_time(real_expr):
            continue
        rnd_query = query_obj.make_twin_aggfltr_query(
            syn_tbl_lst, real_expr, real_groupby_lst, real_from_tbl, real_join_tbl_lst, agg_fntn_terms)
        matched_query = query_obj._match_twin_query(rnd_query)
        scored_query = query_obj.calc_dist_scores(matched_query)
        queries.append(scored_query)
        k += 1
        print('Generated Random Aggregate Filter Query - {} '.format(str(k)))


    rprtr=QryRprt(real_tbl_lst, queries)
    rprtr.print_html_mltpl('cms_cust.html')
    rprtr.plot_violin('Hellinger','cms_cust_hlngr.png' )
    rprtr.plot_violin('Euclidean','cms_cust_ecldn.png' )

