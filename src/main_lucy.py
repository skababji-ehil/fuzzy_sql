import matplotlib
matplotlib.use('Agg')

import sys
sys.path.append('/home/samer/projects/fuzzy_sql/src')

from fuzzy_sql.rnd_query import *
import json
import os
from pathlib import Path



if __name__ == "__main__":
    # set directories
    root_dir = Path('/home/samer/projects/fuzzy_sql')
    metadata_dir = os.path.join(root_dir, 'data/lucy/processed/metadata')
    db_path = os.path.join(root_dir, 'db/lucy.db')

    # define input tables and metadata
    real_tbl_lst = ["b_sample","l_sample"]
    syn_tbl_lst=['b_sample_syn_01','l_sample_syn_01']

    metadata_lst = []
    for tbl_name in real_tbl_lst:
        with open(os.path.join(metadata_dir, tbl_name+'.json'), 'r') as f:
            metadata_lst.append(json.load(f))

    # connect to db
    conn = sqlite3.connect(db_path)


    rnd_queries=gen_queries(3,conn, real_tbl_lst, metadata_lst,  syn_tbl_lst )


    rprtr=QRY_RPRT(real_tbl_lst, rnd_queries)
    rprtr.print_html_mltpl('smk_lucy.html')
    rprtr.plot_violin('Hellinger','smk_lucy_hlngr.png' )
    rprtr.plot_violin('Euclidean','smk_lucy_ecldn.png' )



