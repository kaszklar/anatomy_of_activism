################################################################################
# Read in user following relationships, create a digraph
# For each user in g.nodes assign corpus, # followers, account age in years,
# # of statuses, screen name of user @ time of scrape, and verified status
#
# Output is a serialized networkx digraph object, stored in GCP
# Cloud Storage
#
# Input:
# repo/data/processed/user_following/*
#
# Output:
# tweethis/raw/all_users_digraph.gpickle
################################################################################
import pandas as pd
import networkx as nx
import os
import glob
import logging
from google.cloud import storage

logging.basicConfig(filename='user_following_graph.log',level=logging.DEBUG,
                    format='%(asctime)s %(message)s')

# ---------------------------------------------------------------------------- #
# Graph
# ---------------------------------------------------------------------------- #
logging.info("begin building digraph of user following relationships")

folder_path = '../data/processed/user_following'
g = nx.DiGraph()

for filename in glob.glob(os.path.join(folder_path, '*.txt')):
    try:
        with open(filename, 'r') as f:
            for line in f:
                try:
                    users = line.split(" ")
                    users.pop(1)
                    u = users[0].strip()

                    for following in users[1:]:
                            g.add_edge(u, following.strip())

                except Exception:
                    logging.debug("error reading line @ user {} in file "
                                  "{}\n".format(u, filename))
                    logging.error(exc_info=True)
                    continue

    except Exception:
        logging.debug("error opening file {}\n".format(filename))
        logging.error(exc_info=True)
        continue

logging.info("finish building digraph of user following relationships")


# ---------------------------------------------------------------------------- #
# In order to assign attributes (corpora, followers, no of tweets), import
# a dataframe of users. Assign the string id as the index of the df.
#
# Note: these are only users who participated in conversations. If a node is
# not present, it is noted as "neither" and given 0 followers, 0 tweets for
# the purposes of plotting.
# ---------------------------------------------------------------------------- #
logging.info("import user attributes")

all_users = pd.read_json("../data/processed/combo_user_df_sept19.json", dtype={
    'id_str' : str})
all_users.set_index('id_str', inplace=True)


# ---------------------------------------------------------------------------- #
# Pare down to just our users
# ---------------------------------------------------------------------------- #
logging.info("begin to pare down graph")

# define the nodes that we want based on all_users.index
nbunch = [n for n in g.nodes() if n in all_users.index]

# h graph is a a sub-selection of g
h = nx.DiGraph(g.subgraph(nbunch))

# delete original graph
del g

logging.info("finished subgraph, deleted supergraph")


# ---------------------------------------------------------------------------- #
# Assign attributes to nodes. All attributes are assigned as python strings
# in order to export the graph in GML format later for analysis in gephi.
# ---------------------------------------------------------------------------- #
logging.info("begin assigning attributes to nodes")

for user in h.nodes:
    # try to assign corpus, otherwise: neither
    try:
        h.nodes[user]['corpus'] = all_users.loc[user, 'corpus']
    except:
        h.nodes[user]['corpus'] = 'neither'
    # try to assign following attribute; otherwise, neither
    try:
        h.nodes[user]['followers'] = str(all_users.loc[user, 'followers_count'])
    except:
        h.nodes[user]['followers'] = '0'
    # try to assign no of statuses; otherwise, neither
    try:
        h.nodes[user]['StatusCount'] = str(all_users.loc[user, 'status_count'])
    except:
        h.nodes[user]['StatusCount'] = '0'
    # try to assign scree_name, else 'None, Error'
    try:
        h.nodes[user]['ScreenName'] = str(all_users.loc[user, 'screen_name'])
    except:
        h.nodes[user]['ScreenName'] = 'None, Error'
    # try to assign age of account, else 0
    try:
        h.nodes[user]['AcctYrs'] = str(all_users.loc[user, 'years_old'])
    except:
        h.nodes[user]['AcctYrs'] = '0'
    # try to assign verified status: 1 true, 0 false, if error, assign 0
    try:
        h.nodes[user]['verified'] = str(all_users.loc[user, 'verified'])
    except:
        h.nodes[user]['verified'] = '0'


logging.info("finish assigning attributes to nodes")

# ---------------------------------------------------------------------------- #
# Save graph to GCP cloud storage as a gpickle
#
# local file: all_users_digraph.gpickle
# bucket name: tweethis
# blob name:  raw/all_users_digraph.gpickle
# ---------------------------------------------------------------------------- #
logging.info("begin save graph of our users to gpickle, protocol 4")

file_name = 'all_users_digraph.gpickle'

nx.write_gpickle(h, file_name, protocol=4)

logging.info("begin to write to GCP cloud storage bucket tweethis")

client = storage.Client()
bucket = client.get_bucket('tweethis')

blob = bucket.blob('raw/'+file_name)

blob.upload_from_filename(file_name)

logging.info("gpickle stored. program terminated")
