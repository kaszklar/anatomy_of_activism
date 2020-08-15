################################################################################
# Split our complete graph into either latinx or todes corpus (exclusive). For
# users exclusive to a corpus, assign each;
# - clustering coeff
# - in degree centrality
# - out degree centrality
# - in degree centrality
# - out degree centrality
# - degree centrality
# - betweenness centrality
# - number of predecessors in other corpus
# - number of successors in other corpus
#
# Inputs
# ------
# tweethis/raw/combo_user_df_sept19.json
# tweethis/raw/all_users_digraph.gpickle
#
# Outputs
# -------
# tweethis/processed/todes_g_exclusive.gpickle
# tweethis/processed/latinx_g_exclusive.gpickle
# tweethis/processed/network_metrics_by_user_df.pickle
################################################################################
import pandas as pd
import networkx as nx
import logging
import os
from google.cloud import storage

logging.basicConfig(filename='network_metrics_by_user.log', level=logging.INFO,
                    format='%(asctime)s %(message)s')

# ---------------------------------------------------------------------------- #
# define graphs, create a dataframe of user nodes (exclusively from todes or
# latinx)
# ---------------------------------------------------------------------------- #
logging.info("read in graph of all users, define users df")

# define Cloud Storage bucket
client = storage.Client()
bucket = client.get_bucket('tweethis')

# read complete user graph
blob = bucket.get_blob('raw/all_users_digraph.gpickle')
output_file_name = "all_users_digraph.gpickle"

# download our gpickle blob - do not expand
with open(output_file_name, "wb") as file_obj:
    blob.download_to_file(file_obj, raw_download=True)
# complete user graph is called all_users
all_users = nx.read_gpickle(output_file_name)

# define graph of all users EXCLUSIVELY in todes corpus
todesbunch = [n for n in all_users.nodes() if
              all_users.nodes().data()[n]['corpus']=='todes']
todes_g = nx.DiGraph(all_users.subgraph(todesbunch))
todes_g.name = 'Todes (exclusive) Graph'

# define graph of all users EXCLUSIVELY in latinx corpus
latinxbunch = [n for n in all_users.nodes() if
               all_users.nodes().data()[n]['corpus']=='latinx']
latinx_g = nx.DiGraph(all_users.subgraph(latinxbunch))
latinx_g.name = 'Latinx (exclusive) Graph'

# delete local gpickle
os.remove(output_file_name)

# define pandas dataframe of our nodes that are exclusively in one corpus or
# the other
nodes_dictionary = {key : data['corpus'] for (key, data) in dict(
    all_users.nodes()).items() if data['corpus'] != 'both'}

users_df = pd.DataFrame.from_dict(nodes_dictionary, orient="index",
                                  columns=['corpus'])

# ---------------------------------------------------------------------------- #
# COUNT PREDECESSORS AND SUCCESSORS IN OTHER CORPUS
# ---------------------------------------------------------------------------- #
logging.info("being counting predecessors and successors in other corpus")

other_pred_count = {}
other_successor_count = {}

for node in all_users.nodes():
    # Note the corpus this node is in. If "both," continue to next node
    try:
        this_corpus = all_users.nodes[node]['corpus']
        if this_corpus == 'both':
            continue
    except:
        logging.exception("No corpus for node {} in all_user graph.".format(
            node))
        continue
    # Try to count all of this node's predecessors that are in the other
    # corpus, skipping those in 'both'. Keep track of this in a python dict
    try:
        c = 0
        for pred in all_users.predecessors(node):
            if all_users.nodes[pred]['corpus'] == 'both':
                continue
            if all_users.nodes[pred]['corpus'] != this_corpus:
                c += 1
        other_pred_count[node] = c
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        logging.exception("error calculating predecessors from other corpus "
                          "for node {}".format(node))

    # Now, for this node, try to count the number of successors that are in
    # the other coprus, discounting nodes in 'both.' Keep track of this in a
    # python dict
    try:
        c = 0
        for successor in all_users.successors(node):
            if all_users.nodes[successor]['corpus'] == 'both':
                continue
            if all_users.nodes[successor]['corpus'] != this_corpus:
                c += 1
        other_successor_count[node] = c
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        logging.exception("error calculating successors in other corpus  for "
                          "node {}".format(node))

try:
    # merge the count of predecessors from other coprus into users_df
    predecessor_other = pd.Series(other_pred_count, name='preds_in_other')
    users_df = users_df.join(predecessor_other, how='left')

    # merge the count of successors in other corpus column into users_df
    other_successor = pd.Series(other_successor_count, name='successors_in_other')
    users_df = users_df.join(other_successor, how='left')
except:
    logging.exception("Problem merging sucessors & preds.")

# delete super graph containing all corpora
del all_users

# ---------------------------------------------------------------------------- #
# CLUSTERING COEFFICIENT
# ---------------------------------------------------------------------------- #
try:
    logging.info("generate & merge todes clustering coeff")
    tcluster = nx.clustering(todes_g)
    tclustering = pd.Series(tcluster, name='t_clustering')
    users_df = users_df.join(tclustering, how='left')
    del tcluster, tclustering

    logging.info("generate & merge latinx cluster")
    lcluster = nx.clustering(latinx_g)
    lclustering = pd.Series(lcluster, name='l_clustering')
    users_df = users_df.join(lclustering, how='left')
    del lcluster, lclustering
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error with clustering coefficient")


# ---------------------------------------------------------------------------- #
# IN DEGREE COUNT
# ---------------------------------------------------------------------------- #
try:
    logging.info("generate & merge todes in degree count")
    tindeg = dict(todes_g.in_degree())
    tindeg_series = pd.Series(tindeg, name='t_in_deg')
    users_df = users_df.join(tindeg_series, how='left')

    logging.info("generate & merge latinx in degree count")
    lindeg = dict(latinx_g.in_degree())
    lindeg_series = pd.Series(lindeg, name='l_in_deg')
    users_df = users_df.join(lindeg_series, how='left')
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error with in degree counts")

# ---------------------------------------------------------------------------- #
# OUT DEGREE COUNT
# ---------------------------------------------------------------------------- #
try:
    logging.info("generate & merge todes out degree count")
    toutdeg = dict(todes_g.out_degree())
    toutdeg_series = pd.Series(toutdeg, name='t_out_deg')
    users_df = users_df.join(toutdeg_series, how='left')

    logging.info("generate & merge latinx out degree count")
    loutdeg = dict(latinx_g.out_degree())
    loutdeg_series = pd.Series(loutdeg, name='l_out_deg')
    users_df = users_df.join(loutdeg_series, how='left')
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error with out degree counts")


# ---------------------------------------------------------------------------- #
# DEGREE CENTRALITY
# ---------------------------------------------------------------------------- #
def neighborhood(g, node, n):
    path_lengths = nx.single_source_dijkstra_path_length(g, node)
    return [node for node, length in path_lengths.items() if length == n]


# ---------------------------------------------------------------------------- #
# DEGREE CENTRALITY
# ---------------------------------------------------------------------------- #
try:
    logging.info("generate & merge todes degree centrality")
    t_deg_cent = nx.degree_centrality(todes_g)
    t_deg_cent_series = pd.Series(t_deg_cent, name='t_deg_central')
    users_df = users_df.join(t_deg_cent_series, how='left')

    logging.info("generate & merge latinx degree centrality")
    l_deg_cent = nx.degree_centrality(latinx_g)
    l_deg_cent_series = pd.Series(l_deg_cent, name='l_deg_central')
    users_df = users_df.join(l_deg_cent_series, how='left')
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error with degree centrality")


# ---------------------------------------------------------------------------- #
# TWO HOP NEIGHBORHOODS
# ---------------------------------------------------------------------------- #
def calc_outbound_nhop(graph, source_node, cutoff=2):
    '''
    Calculate the number of non-unique nodes in the n-hop neighborhood.
    :param graph: networkx directed graph
    :param source_node: The node from which to calculate the size of
    the neighborhood.
    :param cutoff: The number of n-hops to include. Default value is 2.
    :return: int
    '''
    # base case 1: we have gone for two hops
    if cutoff == 0:
        return 0
    # base case 2: we have reached a leave node with no successors
    if sum(1 for _ in graph.successors(source_node)) == 0:
        return 0
    # return count of number of successors plus a recursive call for all
    # successors of this node
    for succ in graph.successors(source_node):
        return sum(1 for _ in graph.successors(source_node)) +\
               calc_outbound_nhop(graph, succ, cutoff-1)

def calc_inbound_nhop(graph, destination_node, cutoff):
    '''
    Calculate the number of non-unique nodes in the n-hop neighborhood.
    :param graph: networkx directed graph
    :param destination_node: The node to calculate the size of the neighborhood.
    :param cutoff: The number of n-hops to  include. Default value of 2.
    :return: int
    '''
    if cutoff == 0:
        return 0
    if sum(1 for _ in graph.predecessors(destination_node)) == 0:
        return 0
    for pred in graph.predecessors(destination_node):
        return sum(1 for _ in graph.predecessors(destination_node)) + \
               calc_inbound_nhop(graph, pred, cutoff-1)

logging.info("begin todes in and out bound 2hop neighborhood calculations")

todes_out_2hop = {}
todes_in_2hop = {}

for node in todes_g.nodes():
    try:
        todes_out_2hop[node] = calc_outbound_nhop(todes_g, node, cutoff=2)
        todes_in_2hop[node] = calc_inbound_nhop(todes_g, node, cutoff=2)
    except nx.NodeNotFound:
        logging.exception("node not found in todes")
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        logging.exception("issues calculating n-hop neighborhoods")

t_out_2hop_series = pd.Series(todes_out_2hop, name='t_out_2hop')
t_in_2hop_series = pd.Series(todes_in_2hop, name='t_in_2hop')

users_df = users_df.join(t_out_2hop_series, how='left')
users_df = users_df.join(t_in_2hop_series, how='left')


logging.info("begin latinx in and out bound two-hop neighborhood calcs")
latinx_out_2hop = {}
latinx_in_2hop = {}

for node in latinx_g.nodes():
    try:
        latinx_out_2hop[node] = calc_outbound_nhop(latinx_g, node, cutoff=2)
        latinx_in_2hop[node] = calc_inbound_nhop(latinx_g, node, cutoff=2)
    except (KeyboardInterrupt, SystemExit):
        raise
    except nx.NodeNotFound:
        logging.exception("node not found in latinx")
    except:
        logging.exception("issues calculating neighborhoods in latinx")

l_out_2hop_series = pd.Series(latinx_out_2hop, name='l_out_2hop')
l_in_2hop_series = pd.Series(latinx_in_2hop, name='l_in_2hop')

users_df = users_df.join(l_out_2hop_series, how='left')
users_df = users_df.join(l_in_2hop_series, how='left')


# ---------------------------------------------------------------------------- #
# IN DEGREE CENTRALITY
# ---------------------------------------------------------------------------- #
try:
    logging.info("generate & merge todes in degree centrality")
    t_indeg_cent = nx.in_degree_centrality(todes_g)
    t_indeg_cent_series = pd.Series(t_indeg_cent, name='t_in_deg_central')
    users_df = users_df.join(t_indeg_cent_series, how='left')

    logging.info("generate & merge latinx in degree centrality")
    l_indeg_cent = nx.in_degree_centrality(latinx_g)
    l_indeg_cent_series = pd.Series(l_indeg_cent, name='l_in_deg_central')
    users_df = users_df.join(l_indeg_cent_series, how='left')
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error with in degree centrality")


# ---------------------------------------------------------------------------- #
# OUT DEGREE CENTRALITY
# ---------------------------------------------------------------------------- #
try:
    logging.info("generate & merge todes out degree centrality")
    t_outdeg_cent = nx.out_degree_centrality(todes_g)
    t_outdeg_cent_series = pd.Series(t_outdeg_cent, name='t_out_deg_central')
    users_df = users_df.join(t_outdeg_cent_series, how='left')

    logging.info("generate & merge latinx out degree centrality")
    l_outdeg_cent = nx.out_degree_centrality(latinx_g)
    l_outdeg_cent_series = pd.Series(l_outdeg_cent, name='l_out_deg_central')
    users_df = users_df.join(l_outdeg_cent_series, how='left')
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error with out degree centrality")


# ---------------------------------------------------------------------------- #
# BETWEENNESS CENTRALITY
# ---------------------------------------------------------------------------- #
try:
    logging.info("generate & merge todes betweenness centrality")
    t_bet_cent = nx.betweenness_centrality(todes_g)
    t_bet_cent_series = pd.Series(t_bet_cent, name='t_bet_central')
    users_df = users_df.join(t_bet_cent_series, how='left')

    logging.info("generate & merge latinx betweenness centrality")
    l_bet_cent = nx.degree_centrality(latinx_g)
    l_bet_cent_series = pd.Series(l_bet_cent, name='l_bet_central')
    users_df = users_df.join(l_bet_cent_series, how='left')
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error with betweenness centrality")

# ---------------------------------------------------------------------------- #
# WRITE OUTPUTS
# For each;
# - define local file name
# - write object to that local file
# - define blob
# - upload that local file to blob
# - delete local file
# ---------------------------------------------------------------------------- #
logging.info("writing outputs")

# LATINX GRAPH
# define local file
lg_file_out = 'latinx_g_exclusive_2.gpickle'
# write graph to local file
nx.write_gpickle(latinx_g, lg_file_out, protocol=4)
# define blob, upload local file to blob
blob = bucket.blob('processed/'+lg_file_out)
blob.upload_from_filename(lg_file_out)
# delete local file
os.remove(lg_file_out)

# TODES GRAPH
tg_file_out = 'todes_g_exclusive_2.gpickle'
nx.write_gpickle(todes_g, tg_file_out, protocol=4)
blob = bucket.blob('processed/'+tg_file_out)
blob.upload_from_filename(tg_file_out)
os.remove(tg_file_out)

# DATAFRAME OF USERS
users_file_out = 'network_metrics_by_user_df.pickle'
users_df.to_pickle(users_file_out)
blob = bucket.blob('processed/'+users_file_out)
blob.upload_from_filename(users_file_out)
os.remove(users_file_out)


logging.info("graphs stored, df of network metrics by user stored. program "
             "terminated.")