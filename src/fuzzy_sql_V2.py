from tabular_fntns import *
from tabular_query import TABULAR_QUERY




#Set paths
root_dir=Path(__file__).parent.parent
os.chdir(root_dir)
real_data_dir=os.path.join(root_dir,'data/real') 
syn_data_dir=os.path.join(root_dir,'data/synthetic') 
metadata_dir=os.path.join(root_dir,'data/metadata')

dt = datetime.datetime.now(datetime.timezone.utc)
utc_time = dt.replace(tzinfo=datetime.timezone.utc)
utc_timestamp = utc_time.timestamp()
run_id=int(utc_timestamp)




# Extract names of real datasets 
real_names=extract_fnames(real_data_dir)
syn_names=find_syn_fnames(syn_data_dir, real_names)


#i=42 for testing dataset C1, i=28 for C2, i=20 for C7
""" Original Dataset"""
real_name='C1' #replace by: for real_name in real_names:... 
if len(syn_names[real_name]) >0:
    syn_name=syn_names[real_name][0]

run_id=real_name+'_'+str(run_id)
run_dir=os.path.join(root_dir, 'runs', run_id)
os.mkdir(run_dir)


# Create database for full tabular datasets
conn = sqlite3.connect('sdgd.db')

#Import original dataset from csv files into db tables using pandas
cur = conn.cursor()
real=load_csv(os.path.join(real_data_dir,real_name+'.csv'))
make_table(real_name, real, conn)
if len(syn_names[real_name]) >0:
    syn=load_csv(os.path.join(syn_data_dir,syn_name+'.csv'))
    make_table(syn_name, syn, conn)
#Import metadata from json files into dictionary
metadata=get_metadata(os.path.join(metadata_dir,real_name+'.json'))


test_tq=TABULAR_QUERY(conn, real_name, metadata)

# Single fltr query
output_id="single_fltr"
agg_fntn=False
queries=test_tq.gen_single_fltr_queries(10, agg_fntn=agg_fntn)
with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
    print_single_fltr_queries(queries, file_writer)
pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))


# Twin fltr query
output_id="twin_fltr"
agg_fntn=True
queries=test_tq.gen_twin_fltr_queries(10, syn_name, agg_fntn=agg_fntn)
scored_queries=test_tq.get_fltr_metrics(queries)
with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
    if agg_fntn:
        print_twin_fltr_queries1(scored_queries, file_writer)
    else:
        print_twin_fltr_queries0(scored_queries, file_writer)
pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))



# Single Agg Query 
output_id="single_agg"
agg_fntn=False
hlngr_dropna=False
queries=test_tq.gen_single_agg_queries(10, agg_fntn=agg_fntn) #returned dictionary of non-matching lists
with open(run_dir+'/sql_{}.html'.format(output_id), 'w') as file_writer:
    print_single_agg_queries(queries, file_writer)
pdf.from_file(run_dir+'/sql_{}.html'.format(output_id),run_dir+'/sql_{}.pdf'.format(output_id))


#twin agg query
output_id="twin_agg"
agg_fntn=True
hlngr_dropna=False
queries=test_tq.gen_twin_agg_queries(10, syn_name, agg_fntn=agg_fntn) #returned dictionary of non-matching lists
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


















# #Construct tabular query object and generate queries
# org_tq=TABULAR_QUERY(conn,real_name, syn_name,metadata)
# queries_org=org_tq.gen_twin_agg_queries(10) #returned dictionary of non-matching lists
# scored_queries_org=org_tq.get_agg_metrics(queries_org)
# hlngr_stats_org=calc_stats(scored_queries_org['hlngr_dist'])
# ecldn_stats_org=calc_stats(scored_queries_org['ecldn_dist'])
# with open(run_dir+'/queries4org.html', 'w') as file_writer:
#     print_agg_queries(scored_queries_org, file_writer)

# pdf.from_file(run_dir+'/queries4org.html',run_dir+'/sql_org_{}_{}.pdf'.format(real_name,str(run_id)))

# fig=plot_violin(np.array(scored_queries_org['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),hlngr_stats_org)
# fig.savefig(run_dir+'/hlngr_org_{}_{}.png'.format(real_name,str(run_id)))


# fig=plot_violin(np.array(scored_queries_org['ecldn_dist']),'Euclidean Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name),ecldn_stats_org)
# fig.savefig(run_dir+'/ecldn_org_{}_{}.png'.format(real_name,str(run_id)))

# scored_queries_org_dropna=org_tq.get_agg_metrics(queries_org,hlngr_dropna=True )
# hlngr_stats_org_dropna=calc_stats(scored_queries_org_dropna['hlngr_dist'])
# with open(run_dir+'/queries4org_dropna.html', 'w') as file_writer:
#     print_agg_queries(scored_queries_org_dropna, file_writer)

# pdf.from_file(run_dir+'/queries4org_dropna.html',run_dir+'/sql_org_dropna_{}_{}.pdf'.format(real_name,str(run_id)))
# fig=plot_violin(np.array(scored_queries_org_dropna['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for {} Dataset'.format(real_name), hlngr_stats_org_dropna)
# fig.savefig(run_dir+'/hlngr_org_dropna_{}_{}.png'.format(real_name,str(run_id)))










# """ Discretized (aka binned) Dataset"""
# disc_real_name=real_name+'_b'
# disc_syn_name=real_name+'_syn_06_b'

# # Convert original to binned and save in database
# disc_real, disc_metadata=discretize_data(real,metadata)
# make_table(disc_real_name, disc_real,conn)
# disc_syn, _=discretize_data(syn,metadata)
# make_table(disc_syn_name,disc_syn,conn)

# #Construct tabular query object and generate queries
# disc_tq=TABULAR_QUERY(conn,disc_real_name, disc_syn_name,disc_metadata)
# queries_disc=disc_tq.gen_twin_agg_queries(3, agg_fntn=False) #returned dictionary of non matching lists
# scored_queries_disc=disc_tq.get_agg_metrics(queries_disc)
# hlngr_stats_disc=calc_stats(scored_queries_disc['hlngr_dist'])

# with open(run_dir+'/queries4disc.html', 'w') as file_writer:
#     print_agg_queries(scored_queries_disc, file_writer)

# pdf.from_file(run_dir+'/queries4disc.html',run_dir+'/sql_disc_{}_{}.pdf'.format(real_name,str(run_id)))
# fig=plot_violin(np.array(scored_queries_disc['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for discretized {} Dataset'.format(real_name),hlngr_stats_disc)
# fig.savefig(run_dir+'/hlngr_disc_{}_{}.png'.format(real_name,str(run_id)))



# scored_queries_disc_dropna=disc_tq.get_agg_metrics(queries_disc,hlngr_dropna=True)
# hlngr_stats_disc_dropna=calc_stats(scored_queries_disc_dropna['hlngr_dist'])

# with open(run_dir+'/queries4disc_dropna.html', 'w') as file_writer:
#     print_agg_queries(scored_queries_disc_dropna, file_writer)

# pdf.from_file(run_dir+'/queries4disc_dropna.html',run_dir+'/sql_disc_dropna_{}_{}.pdf'.format(real_name,str(run_id)))
# fig=plot_violin(np.array(scored_queries_disc_dropna['hlngr_dist']),'Hellinger Dist.','Real-Synthetic Query Comparison for discretized {} Dataset'.format(real_name), hlngr_stats_disc_dropna)
# fig.savefig(run_dir+'/hlngr_disc_dropna_{}_{}.png'.format(real_name,str(run_id)))







