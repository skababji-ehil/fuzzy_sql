from tabular_fntns import *
from tabular_query import TABULAR_QUERY

import os



def fuzz_tabular(n_queries: int, query_type,real_file_path, metadata_file_path, syn_file_path='None', ) -> dict:
    """ The function setups a database and generates 'n_queries' number of random SELECT statements for the single input dataset provided in 'real_file_path'. The input file shall be tabular and in csv format. Make sure to input the full path including the file name and its extension. For Windows users, add 'r' before the path string. For instance, r"C:\path\to\file\file.csv". This ensure that backslashes are treated properly.

    In addition to the dataset, you need to provide its variable description in a separate json file 'metadata_file_path'. Variables can be either 'nominal' (i.e. categorical), 'continuous' or 'date'. The json file has the format:{ "name_of_var1":"type_of var1", "name_of_var1":"type_of var2",...}. Note that the names of the variables shall be identical to the names provided in the corresponding csv files i.e. column headers.
    
    The function also generates random queries for both real and synthetic datasets (i.e. twin datasets). The 'query_type' can be one of the following:
    - 'single_agg': This will generate random AGGREGATE queries for the dataset provided in 'real_file_path'. Aggregate queries include GROUP BY clause.
    - 'single_fltr': This will generate random FILTER queries for the dataset provided in 'real_file_path'. FILTER queries include WHERE clause.
    - 'twin_agg': This will generate random AGGREGATE twin queries (i.e. both queries have identical query parameters) for both the input real and synthetic datasets. For 'twin_agg' queries, the function returns the Hellinger and Euclidean distances whenever applicable. 
    - 'twin_fltr': This will generate random FILTER twin queries for both the input real and synthetic datasets.
    
    The function generates the necessary reports in a separate folder. It also returns a dictionary of all the generated queries, query parameters and distance scores, if applicable. The dictionary can be used for further analysis"""


    assert n_queries == int(n_queries), "n_queries must be integer"


    if os.path.isfile(real_file_path):
        real=load_csv(real_file_path)
        real_name=Path(real_file_path).stem
        # Create database and load real data into it
        conn = sqlite3.connect('fuzzy_sql.db')
        make_table(real_name, real, conn)
    else:
        raise Exception('The file {} does not exist !'.format(real_file_path))



    if os.path.isfile(metadata_file_path):
        metadata=get_metadata(metadata_file_path)
    else:
        raise Exception('The file {} does not exist'.format(metadata_file_path))


    if syn_file_path != 'None':
        if os.path.isfile(syn_file_path):
            syn=load_csv(syn_file_path)
            syn_name=Path(syn_file_path).stem
            # Load syn data into the database
            make_table(syn_name, syn, conn)
        else:
                raise Exception('The file {}.csv does not exist!'.format(syn_file_path))

    else:
        if query_type!='single_agg' and query_type!='single_fltr':
            raise Exception("You can not choose query type as 'twin_agg' or 'twin_fltr' unless you provide the file name of the synthetic data. Otherwise, please choose query_type as 'single-agg' or 'single_fltr' ")


    dt = datetime.datetime.now(datetime.timezone.utc)
    utc_time = dt.replace(tzinfo=datetime.timezone.utc)
    utc_timestamp = utc_time.timestamp()
    run_id=int(utc_timestamp)
    run_dir=real_name+'_'+str(run_id)
    os.mkdir(run_dir)
    test_tq=TABULAR_QUERY(conn, real_name, metadata)


    if query_type=='single_fltr':
        # Single fltr query
        output_id="single_fltr"
        #agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        agg_fntn=False
        queries=test_tq.gen_single_fltr_queries(n_queries, agg_fntn=agg_fntn)
        with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
            print_single_fltr_queries(queries, file_writer)
        if os.name=='posix':
            pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
        print("Generated {} random queries and saved results in: {}".format(len(queries['query_real']), run_dir))
        return queries

    elif query_type=='twin_fltr':
        # Twin fltr query
        output_id="twin_fltr"
        #agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        agg_fntn=True
        queries=test_tq.gen_twin_fltr_queries(n_queries, syn_name, agg_fntn=agg_fntn)
        scored_queries=test_tq.get_fltr_metrics(queries)
        with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
            if agg_fntn:
                print_twin_fltr_queries1(scored_queries, file_writer)
            else:
                print_twin_fltr_queries0(scored_queries, file_writer)
        if os.name=='posix':
            pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
        print("Generated {} random twin queries and saved results in: {}".format(len(scored_queries['query_real']), run_dir))
        return scored_queries


    elif query_type=='single_agg':
        # Single Agg Query 
        output_id="single_agg"
        #agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        agg_fntn=False
        hlngr_dropna=False
        queries=test_tq.gen_single_agg_queries(n_queries, agg_fntn=agg_fntn) #returned dictionary of non-matching lists
        with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
            print_single_agg_queries(queries, file_writer)
        if os.name=='posix':
            pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
        print("Generated {} random queries and saved results in: {}".format(len(queries['query_real']), run_dir))
        return queries


    elif query_type=='twin_agg':
        #twin agg query
        output_id="twin_agg"
        #agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        agg_fntn=True
        hlngr_dropna=False
        queries=test_tq.gen_twin_agg_queries(n_queries, syn_name, agg_fntn=agg_fntn) #returned dictionary of non-matching lists
        scored_queries=test_tq.get_agg_metrics(queries, hlngr_dropna=hlngr_dropna)
        with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
            print_twin_agg_queries(scored_queries, file_writer)
        if os.name=='posix':
            pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
        hlngr_stats=calc_stats(scored_queries['hlngr_dist'])
        fig=plot_violin(np.array(scored_queries['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),hlngr_stats)
        fig.savefig(run_dir+'/hlngr_{}.png'.format(output_id))
        if agg_fntn:
            ecldn_stats=calc_stats(scored_queries['ecldn_dist'])
            fig=plot_violin(np.array(scored_queries['ecldn_dist']),'Euclidean Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),ecldn_stats)
            fig.savefig(run_dir+'/ecldn_{}.png'.format(output_id))    
        print("Generated {} random twin queries and saved results in: {}".format(len(scored_queries['query_real']), run_dir))
        return scored_queries

    else:
        raise Exception("Please enter correct query type: 'single_fltr','twin_fltr', 'single_agg, or 'twin_agg' ")



# fuzz_tabular(100,'single_agg', '/home/samer/projects/fuzzy_sql/data/real/oncovid_dtd.csv', '/home/samer/projects/fuzzy_sql/data/metadata/oncovid_dtd.json')
# fuzz_tabular(100,'single_fltr', '/home/samer/projects/fuzzy_sql/data/real/oncovid_dtd.csv', '/home/samer/projects/fuzzy_sql/data/metadata/oncovid_dtd.json')
# fuzz_tabular(100,'twin_fltr', '/home/samer/projects/fuzzy_sql/data/real/C1.csv', '/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')
# fuzz_tabular(100,'twin_agg', '/home/samer/projects/fuzzy_sql/data/real/C1.csv', '/home/samer/projects/fuzzy_sql/data/metadata/C1.json','/home/samer/projects/fuzzy_sql/data/synthetic/C1_syn_06.csv')