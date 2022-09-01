from tabular_fntns import *
import os


if __name__=='__main__':
    
    fuzz_tabular(100,'single_agg', '/home/samer/projects/fuzzy_sql/data/real/oncovid_dtd.csv', '/home/samer/projects/fuzzy_sql/data/metadata/oncovid_dtd.json')
    fuzz_tabular(100,'single_fltr', '/home/samer/projects/fuzzy_sql/data/real/oncovid_dtd.csv', '/home/samer/projects/fuzzy_sql/data/metadata/oncovid_dtd.json')
    fuzz_tabular(100,'twin_fltr', '/home/samer/projects/fuzzy_sql/data/real/C1.csv', '/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')
    fuzz_tabular(100,'twin_agg', '/home/samer/projects/fuzzy_sql/data/real/C1.csv', '/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')



# from tabular_fntns import *
# from tabular_query import TABULAR_QUERY
# import os


# if __name__=='__main__':
#     agg_fntn=True
#     conn = sqlite3.connect('fuzzy_sql.db')
#     class_inputs=setup_class_inputs(conn,'/home/samer/projects/fuzzy_sql/data/real/C1.csv','/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')
#     testq=TABULAR_QUERY(conn,class_inputs['real_data_name'], class_inputs['metadata_dict'])
#     #where_vars=['age','education','income','capital']
#     #testq._gen_single_aggfltr_expr(where_vars, agg_fntn=True)
#     queries=testq.gen_twin_aggfltr_queries(100, class_inputs['syn_data_name'], agg_fntn=agg_fntn)
#     scored_queries=testq.get_agg_metrics(queries, hlngr_dropna=1)
#     with open('sql.html', 'w') as file_writer:
#         print_twin_agg_queries(scored_queries, file_writer)
#     if os.name=='posix':
#         pdf.from_file('sql.html','sql.pdf')
#     hlngr_stats=calc_stats(scored_queries['hlngr_dist'])
#     fig=plot_violin(np.array(scored_queries['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(testq.REAL_TBL_NAME),hlngr_stats)
#     fig.savefig('hlngr.png')
#     if agg_fntn:
#         ecldn_stats=calc_stats(scored_queries['ecldn_dist'])
#         fig=plot_violin(np.array(scored_queries['ecldn_dist']),'Euclidean Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(testq.REAL_TBL_NAME),ecldn_stats)
#         fig.savefig('ecldn.png')    

