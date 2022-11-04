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
    syn_tbl_lst=['s2_ben_sum_2008','s2_ben_sum_2009','s2_ben_sum_2010','s2_carrier_2a','s2_carrier_2b','s2_inpatient','s2_outpatient','s2_prescrp']

    metadata_lst = []
    for tbl_name in real_tbl_lst:
        with open(os.path.join(metadata_dir, tbl_name+'.json'), 'r') as f:
            metadata_lst.append(json.load(f))

    # connect to db
    conn = sqlite3.connect(db_path)


    rnd_queries=gen_mltpl_aggfltr(100,conn, real_tbl_lst, metadata_lst,  syn_tbl_lst, groupby_terms=2, where_terms=3, join_terms=2 )

    reporter=MLTPL_QRY_RPRTR()
    reporter.print_html_mltpl(rnd_queries,'smk_cms.html')

