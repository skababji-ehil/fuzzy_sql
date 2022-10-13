from array import array
from pathlib import Path
import os
import datetime
import sqlite3
import pandas as pd
import numpy as np
import json
import copy
from sklearn.preprocessing import KBinsDiscretizer
#import pdfkit as pdf
import random
import string

from fuzzy_sql.tabular_query import TABULAR_QUERY


import matplotlib.pylab as plt
import seaborn as sns
sns.set_style("ticks",{'axes.grid' : True})

def plot_violin(history: array, xlabel, title, stats_dict):
    fig, ax=plt.subplots(1,1,figsize=(12, 6))
    sns.violinplot(x=history, ax=ax)
    #ax.set_xlim(-0.2,1)
    ax.set_xlabel(xlabel+" ( median: {} , mean: {} , std dev: {} ) ".format(round(stats_dict['median'],2), round(stats_dict['mean'],2),round(stats_dict['stddev'],2)))
    #ax.set_xticks([0,0.2,0.4,0.6,0.8,1.0])
    fig.suptitle(title, fontsize=12)
    return fig



def extract_fnames(data_dir: Path) -> list:
    """ The function extract file names without teh extension for all the files in the input directory 'data_dir. It returns a list with all file names """
    ds_fn=os.listdir(data_dir) # Obtain a list of dataset file names
    ds_name=[Path(ds_fn_i).stem for ds_fn_i in ds_fn]
    print('Extracted the names of {} real datasets'.format(str(len(ds_name))))
    return ds_name

def find_syn_fnames(syn_data_dir: Path, real_names: list) -> dict:
    syn_dict={}
    for real_name_i in real_names:
        catch_file=[]
        for syn_name_i in os.listdir(syn_data_dir):
            if syn_name_i.startswith(real_name_i+'_'):
                catch_file.append(Path(syn_name_i).stem)
            syn_dict[real_name_i]=catch_file
    print('Extracted the names of all available synthetic datasets corresponding to {} real datasets'.format(str(len(syn_dict))))
    return syn_dict


def load_csv(file_path: Path) -> pd.DataFrame:
    """Reads the input csv file.
    
    Args:
        file_path: The input file full path including the file name and csv extension.

    Returns:
        The pandas dataframe in 'unicode-escape' encoding. Any "`" is deleted in the data. 
    """
    df=pd.read_csv(file_path, encoding='unicode-escape', dtype=str) 
    #df=pd.read_csv(file_path,encoding = "ISO-8859-1") 
    #remove any apostrophe from data
    df=df.replace({"'":""}, regex=True) # In order not to encounter error when reading numeric classes e.g. '1' for Class variable in table C4
    return df

def get_metadata(file_path: Path) -> dict:
    """ The function reads the input json file and returns a dictionary """
    metafile= open(file_path)
    return json.load(metafile)

def make_table(table_name: str, df: pd.DataFrame, db_conn: object):
    """Imports the input dataframe into a database table. All dots in the variable names will be replaced by underscores.
    
    Args:
        table_name: The intended name of the table in the database.
        df: The input data
        db_conn: Database (sqlite3) connection object
    """

    #replace any dot in the column names by underscore 
    for i, var in enumerate(list(df.columns)):
        if "." in var:
            df.rename(columns={var:var.replace(".","_")}, inplace=True)
        else:
            continue

    cur=db_conn.cursor()
    cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=(?) ",(table_name,)) #sqlite_master holds  the schema of the db including table names
    if cur.fetchone()[0]==0 : # If table does not exist (ie returned count is zero), then import the table into db from pandas
        df.to_sql(table_name, db_conn, index=False)
        print(f'Table {table_name} is created in the database')
    else:
        print (f'Table {table_name} already exists in the database')
        

def discretize_data( data_org: pd.DataFrame, metadata_org: dict, n_bins=10):
    data=copy.deepcopy(data_org)
    metadata=copy.deepcopy(metadata_org)
    cnt_vars=[key for key,value in metadata.items() if value == 'continuous'] #Get all continuous var names 
    clnd_cnt_vars=[name.replace('`','') for name in cnt_vars]
    data_cnt=data[clnd_cnt_vars]
    discretizer = KBinsDiscretizer(n_bins=n_bins, encode='ordinal', strategy='uniform')
    data_binned=discretizer.fit_transform(data_cnt.values)
    discretizer.feature_names_in_=data_cnt.columns
    data[clnd_cnt_vars]=data_binned
    metadata=metadata.fromkeys(metadata,'nominal')
    return data, metadata

def calc_stats(history_arr):
    #history_arr =history_arr[~np.isnan(history_arr)]
    hist=list(history_arr)
    hist=[x for x in hist if ~np.isnan(x)]
    if len(hist)!=0:
        mean=np.mean(hist)
        median=np.median(hist)
        stddev=np.sqrt(np.var(hist))
    else:
        mean=np.nan
        median=np.nan
        stddev=np.nan
    return {'mean':mean, 'median':median, 'stddev':stddev}


    
def print_single_agg_queries(queries: dict, file_writer):
    all_real = queries['query_real']
    all_params = queries['query_params']
    for real,params in zip(all_real,all_params):
        n_real_records=params['real_n_rcrds']
        # get any 5 records to display from both real and syn queries
        a = list(range(len(real)))
        b = random.sample(a, min(5, len(a)))
        real = real.iloc[b]
        real_style = real.style.set_table_attributes("style='display:inline; margin-right: 20px; font-size: 8pt; text-align:center'").set_caption("Data")


        file_writer.write(real_style.to_html())
        file_writer.write('<br><br>')

        #SMK CHECK
        # if not pd.isnull(params['query_agg_op']):
        file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL:</u></p>')
        #expr = params['query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['real_table_name'], *params['query_cat_vars'])
        file_writer.write('<p style="font-size: 8pt">{}</p>'.format(params['real_sql']))
        
        file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))  

        # else:
        #     file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL:</u></p>')
        #     expr = params['query_tmplt'].format(*params['query_cat_vars'], params['real_table_name'], *params['query_cat_vars'])
        #     file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
        #     file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
            
        file_writer.write("<p>===========================================</p>")

    return file_writer


def print_twin_agg_queries(queries: dict, file_writer):
    all_real = queries['query_real']
    all_syn = queries['query_syn']
    all_params = queries['query_params']
    all_hlngr = queries['hlngr_dist']
    all_ecldn = queries['ecldn_dist']
    for real, syn, params, hlngr, ecldn in zip(all_real, all_syn, all_params, all_hlngr, all_ecldn):
        n_real_records=params['real_n_rcrds']
        n_syn_records=params['syn_n_rcrds']
        # get any 5 records to display from both real and syn queries
        a = list(range(len(real)))
        b = random.sample(a, min(5, len(a)))
        real = real.iloc[b]
        syn = syn.iloc[b]
        real_style = real.style.set_table_attributes("style='display:inline; margin-right: 20px; font-size: 8pt; text-align:center'").set_caption("Real")
        syn_style = syn.style.set_table_attributes("style='display:inline; font-size: 8pt;text-align:center'").set_caption("Synthetic")

        if ecldn > 100:
            real_style.set_properties(**{'color': 'blue'})
            syn_style.set_properties(**{'color': 'blue'})

        if hlngr > 0.5:
            real_style.set_properties(**{'color': 'red'})
            syn_style.set_properties(**{'color': 'red'})

        file_writer.write(real_style.to_html())
        file_writer.write('<br><br>')
        file_writer.write(syn_style.to_html())

        #SMK CHECK
        # if not pd.isnull(params['query_agg_op']):
        file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
        #expr = params['query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['real_table_name'], *params['query_cat_vars'])
        file_writer.write('<p style="font-size: 8pt">{}</p>'.format(params['real_sql']))
        
        file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
        
        file_writer.write('<p style="font-size: 8pt;margin-bottom:-10px"><u>SQL for Synthetic:</u></p>')
        #expr = params['query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['syn_table_name'], *params['query_cat_vars'])
        file_writer.write('<p style="font-size: 8pt">{}</p>'.format(params['syn_sql']))
        
        file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_syn_records)))
        
        file_writer.write('<p style="font-size: 8pt"> Normalized Euclidean distance for ({}): {} </p>'.format( params['query_cnt_var'], str(np.round(ecldn, 2))))
        # else:
        #     file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
        #     expr = params['query_tmplt'].format(*params['query_cat_vars'], params['real_table_name'], *params['query_cat_vars'])
        #     file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
        #     file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
            
        #     file_writer.write('<p style="font-size: 8pt;margin-bottom:-10px"><u>SQL for Synthetic:</u></p>')
        #     expr = params['query_tmplt'].format(*params['query_cat_vars'], params['syn_table_name'], *params['query_cat_vars'])
        #     file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
        #     file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_syn_records)))

        file_writer.write('<p style="font-size: 8pt"> Hellinger Distance: {} </p>'.format(str(np.round(hlngr, 3))))
        file_writer.write("<p>===========================================</p>")

    file_writer.write('<br>')
    hlngr_stats=calc_stats(np.array(all_hlngr))
    file_writer.write('<p style="font-size: 10pt; page-break-before:always"> Hellinger Distance Summary: \n  {} </p>'.format(str(hlngr_stats)))
    if not pd.isnull(params['query_agg_op']):
        ecldn_stats=calc_stats(np.array(all_ecldn))
        file_writer.write('<p style="font-size: 10pt"> Euclidean distance Summary : \n {} </p>'.format(str(ecldn_stats)))
    file_writer.write("<p>===========================================</p>")
    return file_writer

def print_single_fltr_queries(queries: dict, file_writer):
    all_real = queries['query_real']
    all_params = queries['query_params']
    for real, params in zip(all_real, all_params,):
        n_real_records=params['real_n_rcrds']
        real = real.head(5)
        real_style = real.style.set_table_attributes("style='display:inline; margin-right: 20px; font-size: 8pt; text-align:center'").set_caption("Real")


        file_writer.write(real_style.to_html())
        file_writer.write('<br><br>')
    
        # if not pd.isnull(params['query_agg_op']):
        #     file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Input Dataset:</u></p>')
        #     expr = params['real_query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['real_table_name'], *params['query_cat_vars'])
        #     file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
        #     file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
        
        # else:
        file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
        file_writer.write('<p style="font-size: 8pt">{}</p>'.format(params['real_query_tmplt']))
        
        file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))

        file_writer.write("<p>===========================================</p>")

    file_writer.write('<br>')
    file_writer.write("<p>===========================================</p>")
    return file_writer


def print_twin_fltr_queries0(queries: dict, file_writer):
    all_real = queries['query_real']
    all_syn = queries['query_syn']
    all_params = queries['query_params']
    all_hits = queries['hits']
    all_misses = queries['misses']
    for real, syn, params, hits, misses in zip(all_real, all_syn, all_params, all_hits, all_misses):
        n_real_records=params['real_n_rcrds']
        n_syn_records=params['syn_n_rcrds']
        real = real.head(3)
        syn = syn.head(3)
        real_style = real.style.set_table_attributes("style='display:inline; margin-right: 20px; font-size: 8pt; text-align:center'").set_caption("Real")
        syn_style = syn.style.set_table_attributes("style='display:inline; font-size: 8pt;text-align:center'").set_caption("Synthetic")

        if hits > 0:
            real_style.set_properties(**{'color': 'green'})
            syn_style.set_properties(**{'color': 'green'})

        file_writer.write(real_style.to_html())
        file_writer.write('<br><br>')
        file_writer.write(syn_style.to_html())
    
        # if not pd.isnull(params['query_agg_op']):
        #     file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
        #     expr = params['real_query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['real_table_name'], *params['query_cat_vars'])
        #     file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
        #     file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
            
        #     file_writer.write('<p style="font-size: 8pt;margin-bottom:-10px"><u>SQL for Synthetic:</u></p>')
        #     expr = params['syn_query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['syn_table_name'], *params['query_cat_vars'])
        #     file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
        #     file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_syn_records)))
            
        #     file_writer.write('<p style="font-size: 8pt">  Querying synthetic data resulted in ({}) identical matches to real data (HITS)  </p>'.format(  str(hits)))
        #     file_writer.write('<p style="font-size: 8pt"> Querying synthetic data resulted in ({}) mis-matches  to real data (MISSES) </p>'.format( str(misses)))

        # else:
        file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
        file_writer.write('<p style="font-size: 8pt">{}</p>'.format(params['real_query_tmplt']))
        
        file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
        
        file_writer.write('<p style="font-size: 8pt;margin-bottom:-10px"><u>SQL for Synthetic:</u></p>')
        file_writer.write('<p style="font-size: 8pt">{}</p>'.format(params['syn_query_tmplt']))
        
        file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_syn_records)))

        file_writer.write('<p style="font-size: 8pt">  Querying synthetic data resulted in ({}) identical matches to real data (HITS)  </p>'.format(  str(hits)))
        file_writer.write('<p style="font-size: 8pt"> Querying synthetic data resulted in ({}) mis-matches  to real data (MISSES) </p>'.format( str(misses)))

        file_writer.write("<p>===========================================</p>")

    file_writer.write('<br>')
    file_writer.write("<p>===========================================</p>")
    return file_writer




def print_twin_fltr_queries1(queries: dict, file_writer):
    all_real = queries['query_real']
    all_syn = queries['query_syn']
    all_params = queries['query_params']
    all_fnctn_diffs = queries['agg_fnctn_diffs']
    all_count_diffs = queries['count_diffs']
    for real, syn, params, fnctn_diff, count_diff in zip(all_real, all_syn, all_params, all_fnctn_diffs, all_count_diffs):
        n_real_records=params['real_n_rcrds']
        n_syn_records=params['syn_n_rcrds']
        real = real.head(3)
        syn = syn.head(3)
        real_style = real.style.set_table_attributes("style='display:inline; margin-right: 20px; font-size: 8pt; text-align:center'").set_caption("Real")
        syn_style = syn.style.set_table_attributes("style='display:inline; font-size: 8pt;text-align:center'").set_caption("Synthetic")

        file_writer.write(real_style.to_html())
        file_writer.write('<br><br>')
        file_writer.write(syn_style.to_html())
    
        # if not pd.isnull(params['query_agg_op']):
        file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
        file_writer.write('<p style="font-size: 8pt">{}</p>'.format(params['real_query_tmplt']))
        
        file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
        
        file_writer.write('<p style="font-size: 8pt;margin-bottom:-10px"><u>SQL for Synthetic:</u></p>')
        file_writer.write('<p style="font-size: 8pt">{}</p>'.format(params['syn_query_tmplt']))
        
        file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_syn_records)))
        
        file_writer.write('<p style="font-size: 8pt"> Aggregate value difference between real and synthetic: ({})  </p>'.format(  str(fnctn_diff)))
        file_writer.write('<p style="font-size: 8pt"> Count value difference between real and synthetic: ({})  </p>'.format( str(count_diff)))

        # else:
        #     file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
        #     expr = params['real_query_tmplt'].format(*params['query_cat_vars'], params['real_table_name'], *params['query_cat_vars'])
        #     file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
        #     file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
            
        #     file_writer.write('<p style="font-size: 8pt;margin-bottom:-10px"><u>SQL for Synthetic:</u></p>')
        #     expr = params['syn_query_tmplt'].format(*params['query_cat_vars'], params['syn_table_name'], *params['query_cat_vars'])
        #     file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
        #     file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_syn_records)))

        #     file_writer.write('<p style="font-size: 8pt"> Aggregate value difference between real and synthetic: ({})  </p>'.format(  str(fnctn_diff)))
        #     file_writer.write('<p style="font-size: 8pt"> Count value difference between real and synthetic: ({}) </p>'.format( str(count_diff)))

        file_writer.write("<p>===========================================</p>")

    file_writer.write('<br>')
    file_writer.write("<p>===========================================</p>")
    return file_writer

    


import string

def fuzz_tabular(n_queries, query_type,real_file_path, metadata_file_path, syn_file_path='None', run_folder='None', printme=False, db_path='fuzzy_sql.db'):
    """The function generates random queries for the input tabular datasets.

    :param int n_queries: The number of random queries to be generated 
    :param str query_type: The type of queries to be generated and can be 'single_agg', single_fltr', 'twin_agg', 'twin_fltr', or 'twin_aggfltr'
    :param path real_file_path: The full path to the real data csv file including the file extension. 
    :param path metadata_file_path: The full path to the metadata json file including the file extension. 
    :param path syn_file_path: The full path to the synthetic data csv file including the file extension or 'None' if random queries are desired for single dataset.
    :param str run_folder: The full path for the your output folder or 'None', which will save the output reports to the current folder.
    :param logical printme: Set it to True if an html report is desired. The report lists random records of all the generated queries.
    :return: A dictionary of all generated random queries. 
    :rtype: dictionary

    """

    assert n_queries == int(n_queries), "n_queries must be integer"

    if os.path.isfile(real_file_path):
        real=load_csv(real_file_path)
        real_name=Path(real_file_path).stem
        # Create database and load real data into it
        conn = sqlite3.connect(db_path)
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
    if printme:
        run_id=int(utc_timestamp)
        run_dir=real_name+'_'+str(run_id)
        if run_folder != 'None':
            run_dir=os.path.join(run_folder, run_dir)
        os.mkdir(run_dir)

    
    test_tq=TABULAR_QUERY(conn, real_name, metadata)

    if query_type=='single_fltr':
        # Single fltr query
        output_id="single_fltr"
        #agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        agg_fntn=False
        queries=test_tq.gen_single_fltr_queries(n_queries, agg_fntn=agg_fntn)

        if printme:
            with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
                print_single_fltr_queries(queries, file_writer)
            # if os.name=='posix':
            #     pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
            print("Generated {} random queries and saved results in: {}".format(len(queries['query_real']), run_dir))
        
        return queries

    elif query_type=='twin_fltr':
        # Twin fltr query
        output_id="twin_fltr"
        #agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        agg_fntn=True
        queries=test_tq.gen_twin_fltr_queries(n_queries, syn_name, agg_fntn=agg_fntn)
        scored_queries=test_tq.get_fltr_metrics(queries)
        if printme:
            with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
                if agg_fntn:
                    print_twin_fltr_queries1(scored_queries, file_writer)
                else:
                    print_twin_fltr_queries0(scored_queries, file_writer)
            # if os.name=='posix':
            #     pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
            print("Generated {} random twin queries and saved results in: {}".format(len(scored_queries['query_real']), run_dir))
        
        return scored_queries

    elif query_type=='single_agg':
        # Single Agg Query 
        output_id="single_agg"
        #agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        agg_fntn=False
        queries=test_tq.gen_single_agg_queries(n_queries, agg_fntn=agg_fntn) #returned dictionary of non-matching lists
        if printme:
            with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
                print_single_agg_queries(queries, file_writer)
            # if os.name=='posix':
            #     pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
            print("Generated {} random queries and saved results in: {}".format(len(queries['query_real']), run_dir))
        
        return queries

    elif query_type=='twin_agg':
        #twin agg query
        output_id="twin_agg"
        #agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        agg_fntn=True
        queries=test_tq.gen_twin_agg_queries(n_queries, syn_name, agg_fntn=agg_fntn) #returned dictionary of non-matching lists
        scored_queries=test_tq.get_agg_metrics(queries)
        hlngr_stats=calc_stats(scored_queries['hlngr_dist'])
        if printme:
            with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
                print_twin_agg_queries(scored_queries, file_writer)
            # if os.name=='posix':
            #     pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))
            fig=plot_violin(np.array(scored_queries['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),hlngr_stats)
            fig.savefig(run_dir+'/hlngr_{}.png'.format(output_id))
            if agg_fntn:
                ecldn_stats=calc_stats(scored_queries['ecldn_dist'])
                fig=plot_violin(np.array(scored_queries['ecldn_dist']),'Euclidean Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),ecldn_stats)
                fig.savefig(run_dir+'/ecldn_{}.png'.format(output_id))    
            print("Generated {} random twin queries and saved results in: {}".format(len(scored_queries['query_real']), run_dir))
        
        return scored_queries
    
    
    elif query_type=='twin_aggfltr':
        output_id="twin_aggfltr"
        agg_fntn=False if len(test_tq.CNT_VARS)==0 else random.randint(0,1)
        queries=test_tq.gen_twin_aggfltr_queries(n_queries, syn_name, agg_fntn=test_tq.AGG_FNCTN)
        scored_queries=test_tq.get_agg_metrics(queries)

        if printme:
            with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
                print_twin_agg_queries(scored_queries, file_writer)

        hlngr_stats=calc_stats(scored_queries['hlngr_dist'])

        if printme:
            fig=plot_violin(np.array(scored_queries['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),hlngr_stats)
            fig.savefig(run_dir+'/hlngr_{}.png'.format(output_id))
            print("Generated {} random twin queries and saved results in: {}".format(len(scored_queries['query_real']), run_dir))  

        
        
        if agg_fntn:
            ecldn_stats=calc_stats(scored_queries['ecldn_dist'])
            if printme:
                fig=plot_violin(np.array(scored_queries['ecldn_dist']),'Euclidean Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),ecldn_stats)
                fig.savefig(run_dir+'/ecldn_{}.png'.format(output_id)) 

        return scored_queries

    else:
        raise Exception("Please enter correct query type: 'single_fltr','twin_fltr', 'single_agg, 'twin_agg' or 'twin_aggfltr' ")

##########################################################################################################################################################

def long_load_csv(file_path: Path) -> pd.DataFrame:
    """Reads the input csv file as strings. Any dot "." in variable names will be replaced into underscore "_"
    
    Args:
        file_path: The input file full path including the file name and csv extension.

    Returns:
        The pandas dataframe in 'unicode-escape' encoding. Any "`" is deleted in the data. 
    """
    df=pd.read_csv(file_path, encoding='unicode-escape', dtype=str) 
    #df=pd.read_csv(file_path,encoding = "ISO-8859-1") 
    #remove any apostrophe from data
    df=df.replace({"'":""}, regex=True) # In order not to encounter error when reading numeric classes e.g. '1' for Class variable in table C4
    df.columns=[col.replace(".","_") for col in df.columns] #replace dots in variable names by _ to avoid conflicts in sql naming 
    return df


def assign_dtype(df, dict):
    #Assigns dtypes for variables in input pandas dataframe based on the type identified in the input dictionary
    assert bool(set(df.columns).intersection(set(dict.keys())))
    out_dict={}
    for key in dict:
        if dict[key] in ['quantitative','continuous','interval','ratio']:
            out_dict[key]='float64'
        elif dict[key] in ['date','time','datetime']:
            out_dict[key]='datetime64'
        else:
            out_dict[key]='category'
    
    for col in df.columns:
        df[col]=df[col].astype(out_dict[col])

    return df



################################################################# GEN RND_QUERY FUNCTIONS START ##############################


