from genericpath import isfile
from tabular_fntns import *
from tabular_query import TABULAR_QUERY

import os


def fuzzy_sql(n_queries: int, real_name: str, syn_name='None', query_type= 'single_agg') -> dict:
    """ The function generates (n_queries) number of randomly generated SELECT queries for the input datasets. The input datasets named (real_name) shall be tabular and saved earlier in csv format in the folder /data/real. The file name shall be identical to the input (real_name). The function also generates random twin queries for both real and twin synthetic datasets. The synthetic dataset shall saved in /data/synthetic. The query_type can be one of the following: 'single_agg', 'single_fltr', 'twin_agg', 'twin_fltr'.
    The function generates the necessary reports and a dictionary of all generated queries, query parameters and distance scores, if applicable."""
    
    assert n_queries == int(n_queries), "n_queries must be integer"
    # Set paths
    root_dir=Path(__file__).parent.parent
    os.chdir(root_dir)
    real_data_dir=os.path.join(root_dir,'data/real') 
    syn_data_dir=os.path.join(root_dir,'data/synthetic') 
    metadata_dir=os.path.join(root_dir,'data/metadata')

    dt = datetime.datetime.now(datetime.timezone.utc)
    utc_time = dt.replace(tzinfo=datetime.timezone.utc)
    utc_timestamp = utc_time.timestamp()
    run_id=int(utc_timestamp)


    run_id=real_name+'_'+str(run_id)
    run_dir=os.path.join(root_dir, 'runs', run_id)
    os.mkdir(run_dir)

    real_file_path=os.path.join(real_data_dir,real_name+'.csv')
    if os.path.isfile(real_file_path):
        real=load_csv(real_file_path)
        # Create database and load real data into it
        conn = sqlite3.connect('fuzzy_sql.db')
        make_table(real_name, real, conn)
    else:
        raise Exception('The file {}.csv does not exist in the directory /data/real!'.format(real_name))


    metafile_path=os.path.join(metadata_dir,real_name+'.json')
    if os.path.isfile(metafile_path):
        metadata=get_metadata(metafile_path)

    else:
        raise Exception('The file {}.json does not exist in the directory /data/metadata!'.format(real_name))


    if syn_name != 'None':
        syn_file_path=os.path.join(syn_data_dir,syn_name+'.csv')
        if os.path.isfile(syn_file_path):
            syn=load_csv(syn_file_path)
            # Load syn data into the database
            make_table(syn_name, syn, conn)
        else:
                raise Exception('The file {}.csv does not exist in the directory /data/synthetic!'.format(syn_name))

    else:
        if query_type!='single_agg' and query_type!='single_fltr':
            raise Exception("You can not choose query type as 'twin_agg' or 'twin_fltr' unless you provide the file name of the synthetic data. Otherwise, please choose query_type as 'single-agg' or 'single_fltr' ")


    test_tq=TABULAR_QUERY(conn, real_name, metadata)

    if query_type=='single_fltr':
        # Single fltr query
        output_id="single_fltr"
        agg_fntn=False
        queries=test_tq.gen_single_fltr_queries(n_queries, agg_fntn=agg_fntn)
        with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
            print_single_fltr_queries(queries, file_writer)
        pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
        print("Generated {} random queries and saved results in: /runs/{}".format(len(queries['query_real']), run_id))
        return queries

    elif query_type=='twin_fltr':
        # Twin fltr query
        output_id="twin_fltr"
        agg_fntn=True
        queries=test_tq.gen_twin_fltr_queries(n_queries, syn_name, agg_fntn=agg_fntn)
        scored_queries=test_tq.get_fltr_metrics(queries)
        with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
            if agg_fntn:
                print_twin_fltr_queries1(scored_queries, file_writer)
            else:
                print_twin_fltr_queries0(scored_queries, file_writer)
        pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
        print("Generated {} random twin queries and saved results in: /runs/{}".format(len(scored_queries['query_real']), run_id))
        return scored_queries


    elif query_type=='single_agg':
        # Single Agg Query 
        output_id="single_agg"
        agg_fntn=False
        hlngr_dropna=False
        queries=test_tq.gen_single_agg_queries(n_queries, agg_fntn=agg_fntn) #returned dictionary of non-matching lists
        with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
            print_single_agg_queries(queries, file_writer)
        pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
        print("Generated {} random queries and saved results in: /runs/{}".format(len(queries['query_real']), run_id))
        return queries


    elif query_type=='twin_agg':
        #twin agg query
        output_id="twin_agg"
        agg_fntn=True
        hlngr_dropna=False
        queries=test_tq.gen_twin_agg_queries(n_queries, syn_name, agg_fntn=agg_fntn) #returned dictionary of non-matching lists
        scored_queries=test_tq.get_agg_metrics(queries, hlngr_dropna=hlngr_dropna)
        with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
            print_twin_agg_queries(scored_queries, file_writer)
        pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
        hlngr_stats=calc_stats(scored_queries['hlngr_dist'])
        fig=plot_violin(np.array(scored_queries['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),hlngr_stats)
        fig.savefig(run_dir+'/hlngr_{}.png'.format(output_id))
        if agg_fntn:
            ecldn_stats=calc_stats(scored_queries['ecldn_dist'])
            fig=plot_violin(np.array(scored_queries['ecldn_dist']),'Euclidean Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),ecldn_stats)
            fig.savefig(run_dir+'/ecldn_{}.png'.format(output_id))    
        print("Generated {} random twin queries and saved results in: /runs/{}".format(len(scored_queries['query_real']), run_id))
        return scored_queries


#queries=fuzzy_sql(10, 'C1','C1_syn_06', query_type='twin_fltr')