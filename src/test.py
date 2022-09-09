from fuzzy_sql.fuzzy_sql import *
import os  

#set paths
root_dir=Path(os.getcwd())
real_dir=os.path.join(root_dir,'data/tabular/real')
meta_dir=os.path.join(root_dir,'data/tabular/metadata')
syn_dir=os.path.join(root_dir,'data/tabular/synthetic')

#extract real data names and define paths
ds_names=extract_fnames(real_dir)
# drop trail 5 since there is no synth data for it
ds_names.remove('trial5')
ds_names.remove('oncovid_dtd')
ds_names.remove('danish')

real_path=[]
meta_path=[]
syn_path=[]
for ds_name in ds_names:
    real_path.append(real_dir+f'/{ds_name}.csv')
    meta_path.append(meta_dir+f'/{ds_name}.json')
    syn_path.append(syn_dir+f'/{ds_name}_syn_06.csv')




    # Fuzz
res={'name':[], 'hlngr_mean':[], 'hlngr_median':[], 'hlngr_std_dev':[], 'ecldn_mean':[], 'ecldn_median':[], 'ecldn_std_dev':[] }
queries=[]
for i,ds_name in enumerate(ds_names):
    # if ds_name !='danish':
    #     continue
    scored_queries=fuzz_tabular(10,'twin_aggfltr', real_path[i], meta_path[i],syn_path[i])
    queries.append(scored_queries)
    # res['name'].append(ds_name)
    # hlngr_stats=calc_stats(scored_queries['hlngr_dist'])
    # res['hlngr_mean'].append(hlngr_stats['mean'])
    # res['hlngr_median'].append(hlngr_stats['median'])
    # res['hlngr_std_dev'].append(hlngr_stats['stdev'])
    # ecldn_stats=calc_stats(scored_queries['ecldn_dist'])
    # res['ecldn_mean'].append(hlngr_stats['mean'])
    # res['ecldn_median'].append(hlngr_stats['median'])
    # res['ecldn_std_dev'].append(hlngr_stats['stdev'])

print('smk')