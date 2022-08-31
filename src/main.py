# from tabular_fntns import *
# import os


# if __name__=='__main__':
    
#     fuzz_tabular(100,'single_agg', '/home/samer/projects/fuzzy_sql/data/real/oncovid_dtd.csv', '/home/samer/projects/fuzzy_sql/data/metadata/oncovid_dtd.json')
#     fuzz_tabular(100,'single_fltr', '/home/samer/projects/fuzzy_sql/data/real/oncovid_dtd.csv', '/home/samer/projects/fuzzy_sql/data/metadata/oncovid_dtd.json')
#     fuzz_tabular(100,'twin_fltr', '/home/samer/projects/fuzzy_sql/data/real/C1.csv', '/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')
#     fuzz_tabular(100,'twin_agg', '/home/samer/projects/fuzzy_sql/data/real/C1.csv', '/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')



from tabular_fntns import *
from tabular_query import TABULAR_QUERY
import os


if __name__=='__main__':
    conn = sqlite3.connect('fuzzy_sql.db')
    class_inputs=setup_class_inputs(conn,'/home/samer/projects/fuzzy_sql/data/real/C1.csv','/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')
    testq=TABULAR_QUERY(conn,class_inputs['real_data_name'], class_inputs['metadata_dict'])

    #where_vars=['age','education','income','capital']
    #testq._gen_single_aggfltr_expr(where_vars, agg_fntn=True)
    queries=testq.gen_single_aggfltr_queries(100)


