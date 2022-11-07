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
    metadata_dir = os.path.join(root_dir, 'data/sdgd/processed/metadata')
    db_path = os.path.join(root_dir, 'db/sdgd.db')
    run_dir=os.path.join(root_dir,'.runs')

    # define input tables and metadata
    real_tbl_lst = ["C1"]
    syn_tbl_lst = ['C1_syn_default_1']

    metadata_lst = []
    for tbl_name in real_tbl_lst:
        with open(os.path.join(metadata_dir, tbl_name+'.json'), 'r') as f:
            metadata_lst.append(json.load(f))

    # connect to db
    conn = sqlite3.connect(db_path)

    rnd_queries=gen_queries(3,conn, real_tbl_lst, metadata_lst,  syn_tbl_lst )


    rprtr=QRY_RPRT(real_tbl_lst, rnd_queries)
    rprtr.print_html_mltpl('smk_sdgd.html')
    rprtr.plot_violin('Hellinger','smk_sdgd_hlngr.png' )
    rprtr.plot_violin('Euclidean','smk_sdgd_ecldn.png' )




