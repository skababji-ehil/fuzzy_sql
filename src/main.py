from tabular_fntns import *
import os


if __name__=='__main__':
    
    (100,'single_agg', '/home/samer/projects/fuzzy_sql/data/real/oncovid_dtd.csv', '/home/samer/projects/fuzzy_sql/data/metadata/oncovid_dtd.json')
    fuzz_tabular(100,'single_fltr', '/home/samer/projects/fuzzy_sql/data/real/oncovid_dtd.csv', '/home/samer/projects/fuzzy_sql/data/metadata/oncovid_dtd.json')
    fuzz_tabular(100,'twin_fltr', '/home/samer/projects/fuzzy_sql/data/real/C1.csv', '/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')
    fuzz_tabular(100,'twin_agg', '/home/samer/projects/fuzzy_sql/data/real/C1.csv', '/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')