from fuzzy_sql.fuzzy_sql import *


real_path1='/home/samer/projects/fuzzy_sql/data/tabular/ready/real/oncovid_dtd.csv'
meta_path1='/home/samer/projects/fuzzy_sql/data/tabular/ready/metadata/oncovid_dtd.json'

real_path2='/home/samer/projects/fuzzy_sql/data/tabular/ready/real/C1.csv'
meta_path2='/home/samer/projects/fuzzy_sql/data/tabular/ready/metadata/C1.json'
syn_path2='/home/samer/projects/fuzzy_sql/data/tabular/ready/synthetic/C1_syn_06.csv'

real_path3='/home/samer/projects/fuzzy_sql/data/tabular/ready/real/C7.csv'
meta_path3='/home/samer/projects/fuzzy_sql/data/tabular/ready/metadata/C7.json'
syn_path3='/home/samer/projects/fuzzy_sql/data/tabular/ready/synthetic/C7_syn_06.csv'


# fuzz_tabular(1000,'single_agg',real_path1 ,meta_path1 , run_folder='.runs')
# fuzz_tabular(1000,'single_fltr', real_path1, meta_path1,run_folder='.runs')
# fuzz_tabular(1000,'twin_fltr',real_path2 ,meta_path2 ,syn_path2,run_folder='.runs')
fuzz_tabular(1000,'twin_agg', real_path2, meta_path2,syn_path2,run_folder='.runs')
# fuzz_tabular(1000,'twin_aggfltr',real_path3 ,meta_path3 ,syn_path3,run_folder='.runs')
