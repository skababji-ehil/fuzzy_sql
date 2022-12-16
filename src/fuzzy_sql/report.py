from pathlib import Path
from typing import Union, Tuple
from textwrap import wrap
import numpy as np
import matplotlib.pylab as plt
import seaborn as sns
sns.set_style("ticks", {'axes.grid': True})


import matplotlib
matplotlib.use('Agg')




class Report():

    def __init__(self, dataset_table_lst: list, random_queries: dict):
        """ Generates reports and plots for the input random queries. 

        Args:
            dataset_table_lst (list): List of table names that were used to generte the input queries.
            random_queries (dict) : A dictionary comprising multiple number of queries as datasframes with detailed desciption for each query. 

        """

        self.tbl_lst = dataset_table_lst
        self.rnd_queries = random_queries

        self.style = """
            .mystyle {
                font-size: 7pt; 
                font-family: Arial;
                border-collapse: collapse; 
                border: 1px solid silver;
            /*     margin-left: auto;
                margin-right: auto; */
                
            }

            .mystyle td, th {
                padding: 5px;
                text-align: center;
            }

            .mystyle tr:nth-child(even) {
                background: #E0E0E0;
            }

            .mystyle tr:hover {
                background: silver;
                cursor: pointer;
            }


            ul li {
                font-size: 7px;
            }

            p {
                color: navy;
                text-indent: 7px;
            }
        """

        self.start_html = f"<!DOCTYPE html><html><head><style>{self.style}</style></head><body><H1>Random Queries Generated by Fuzzy SQL</H1>"
        self.end_html = "</body></html>"

    def query_to_html(self, query_id: str, rnd_query: dict) -> str:
        assert query_id == 'real' or query_id == 'syn', (
            "query_id shall be either 'real' or 'syn' ")
        html_string = f"<u>SQL statement - {query_id}:</u><br>"
        html_string += rnd_query['query_desc'][f'sql_{query_id}']
        html_string += "<br><br>"
        if len(rnd_query[f'query_{query_id}']) != 0:
            html_string += f"SQL result - {query_id}:<br>"
            html_string += rnd_query[f'query_{query_id}'].head(
                5).to_html(classes='mystyle')
            html_string += "Number of returned records: " + \
                str(rnd_query['query_desc'][f'n_rows_{query_id}'])
        else:
            html_string += f"<H4>No records returned</H4>"
        html_string += "<br><br>"
        return html_string

    def print_html_mltpl(self, output_file: Path):
        with open(output_file, 'w') as f:
            f.write(self.start_html)
            for query in self.rnd_queries:
                f.write(
                    f"<H3>======================= START RANDOM QUERY ======================</H3>")
                f.write(self.query_to_html('real', query))
                f.write(self.query_to_html('syn', query))
                f.write("Hellinger Distance = {:.3f}".format(
                    query['query_hlngr_score']))
                f.write("<br>")
                f.write("Normalized Euclidean Distance = {:.3f}".format(
                    query['query_ecldn_score']))
                f.write(
                    "<H3>************************************************************************************</H3>")
            f.write(self.end_html)

    def calc_stats(self) -> Tuple[dict, dict]:
        #history_arr =history_arr[~np.isnan(history_arr)]
        hlngr_lst = [self.rnd_queries[i]['query_hlngr_score']
                     for i in range(len(self.rnd_queries))]
        ecldn_lst = [self.rnd_queries[i]['query_ecldn_score']
                     for i in range(len(self.rnd_queries))]

        hlngr_lst = [x for x in hlngr_lst if ~np.isnan(x)]
        ecldn_lst = [x for x in ecldn_lst if ~np.isnan(x)]

        if len(hlngr_lst) != 0:
            mean = np.mean(hlngr_lst)
            median = np.median(hlngr_lst)
            stddev = np.sqrt(np.var(hlngr_lst))
        else:
            mean = np.nan
            median = np.nan
            stddev = np.nan
        hlngr_stats = {'mean': mean, 'median': median, 'stddev': stddev}

        if len(ecldn_lst) != 0:
            mean = np.mean(ecldn_lst)
            median = np.median(ecldn_lst)
            stddev = np.sqrt(np.var(ecldn_lst))
        else:
            mean = np.nan
            median = np.nan
            stddev = np.nan
        ecldn_stats = {'mean': mean, 'median': median, 'stddev': stddev}

        return hlngr_stats, ecldn_stats

    def plot_violin(self, type: str, outputfile: str):
        hlngr_stats, ecldn_stats = self.calc_stats()
        if type == 'Hellinger':
            lst = [self.rnd_queries[i]['query_hlngr_score']
                   for i in range(len(self.rnd_queries))]
            stats = hlngr_stats
        elif type == 'Euclidean':
            lst = [self.rnd_queries[i]['query_ecldn_score']
                   for i in range(len(self.rnd_queries))]
            stats = ecldn_stats
        else:
            raise ValueError('type shall be either Hellinger or Euclidean')

        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        sns.violinplot(x=lst, ax=ax)
        # ax.set_xlim(-0.2,1)
        ax.set_xlabel(type + " ( median: {} , mean: {} , std dev: {} ) ".format(
            round(stats['median'], 2), round(stats['mean'], 2), round(stats['stddev'], 2)))
        # ax.set_xticks([0,0.2,0.4,0.6,0.8,1.0])
        fig.suptitle(
            "\n".join(wrap(f'Fuzzy SQL for {self.tbl_lst}')), fontsize=12)
        fig.savefig(outputfile)
        # fig.show()

