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
import pdfkit as pdf
import random


import matplotlib.pylab as plt
import seaborn as sns
sns.set_style("ticks",{'axes.grid' : True})



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


def load_csv(file_path: Path) -> list:
    df=pd.read_csv(file_path,encoding = "ISO-8859-1") 
    #remove any apostrophe from data
    df=df.replace({"'":""}, regex=True)
    return df

def get_metadata(file_path: Path) -> dict:
    """ The function reareal the input json file and returns a dictionary """
    metafile= open(file_path)
    return json.load(metafile)

def make_table(real_name: list, real: list, db_conn):
    """" The function converts input dataframe into a table using the input database cursor """
    cur=db_conn.cursor()
    cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=(?) ",(real_name,)) #sqlite_master holds  the schema of the db including table names
    if cur.fetchone()[0]==0 : # If table does not exist (ie returned count is zero), then import the table into db from pandas
        real.to_sql(real_name, db_conn, index=False)
        print(f'Table {real_name} is created in the database')
    else:
        print (f'Table {real_name} already exists in the database')


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
    mean=np.mean(history_arr)
    median=np.median(history_arr)
    stddev=np.sqrt(np.var(history_arr))
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
    
        if not pd.isnull(params['query_agg_op']):
            file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL:</u></p>')
            expr = params['query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['real_table_name'], *params['query_cat_vars'])
            file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
            file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))  

        else:
            file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL:</u></p>')
            expr = params['query_tmplt'].format(*params['query_cat_vars'], params['real_table_name'], *params['query_cat_vars'])
            file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
            file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
            
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
    
        if not pd.isnull(params['query_agg_op']):
            file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
            expr = params['query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['real_table_name'], *params['query_cat_vars'])
            file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
            file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
            
            file_writer.write('<p style="font-size: 8pt;margin-bottom:-10px"><u>SQL for Synthetic:</u></p>')
            expr = params['query_tmplt'].format(*params['query_cat_vars'], params['query_agg_op'], params['query_cnt_var'], params['syn_table_name'], *params['query_cat_vars'])
            file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
            file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_syn_records)))
            
            file_writer.write('<p style="font-size: 8pt"> Normalized Euclidean distance for ({}): {} </p>'.format( params['query_cnt_var'], str(np.round(ecldn, 2))))
        else:
            file_writer.write('<p style="font-size: 8pt; margin-bottom:-10px"><u>SQL for Real:</u></p>')
            expr = params['query_tmplt'].format(*params['query_cat_vars'], params['real_table_name'], *params['query_cat_vars'])
            file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
            file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_real_records)))
            
            file_writer.write('<p style="font-size: 8pt;margin-bottom:-10px"><u>SQL for Synthetic:</u></p>')
            expr = params['query_tmplt'].format(*params['query_cat_vars'], params['syn_table_name'], *params['query_cat_vars'])
            file_writer.write('<p style="font-size: 8pt">{}</p>'.format(expr))
            
            file_writer.write('<p style="font-size: 8pt; margin-top:-5px"> Resulted in {} records</p>'.format(str(n_syn_records)))

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

    
def plot_violin(history: array, xlabel, title, stats_dict):
    fig, ax=plt.subplots(1,1,figsize=(12, 6))
    sns.violinplot(x=history, ax=ax)
    #ax.set_xlim(-0.2,1)
    ax.set_xlabel(xlabel+" ( median: {} , mean: {} , std dev: {} ) ".format(round(stats_dict['median'],2), round(stats_dict['mean'],2),round(stats_dict['stddev'],2)))
    #ax.set_xticks([0,0.2,0.4,0.6,0.8,1.0])
    fig.suptitle(title, fontsize=12)
    return fig
