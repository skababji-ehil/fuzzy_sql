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

    # define input tables and metadata
    real_tbl_lst = ["C1"]
    syn_tbl_lst = ['C1_syn_default_1']

    metadata_lst = []
    for tbl_name in real_tbl_lst:
        with open(os.path.join(metadata_dir, tbl_name+'.json'), 'r') as f:
            metadata_lst.append(json.load(f))

    # connect to db
    conn = sqlite3.connect(db_path)


    twin_queries=gen_mltpl_aggfltr(10,conn, real_tbl_lst, metadata_lst,  syn_tbl_lst )

