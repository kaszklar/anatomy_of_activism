################################################################################
# Calculate overall reciprocity for todes following graph and latinx
# following graph. Append these metrics to network_metrics.txt.
# Calculate node-level reciprocity metric and join to user level network
# metrics.
#
# Inputs
# ------
# tweethis/processed/todes_g_exclusive.gpickle
# tweethis/processed/latinx_g_exclusive.gpickle
# tweethis/processed/network_metrics_by_user.pickle
#
# Outputs
# -------
# tweethis/processed/network_metrics.txt
# tweethis/processed/network_metrics_by_user.pickle
################################################################################
import pandas as pd
import networkx as nx
import logging
import os
from google.cloud import storage
from datetime import datetime

this_file = "reciprocity"

logging.basicConfig(filename=this_file+'.log', level=logging.INFO,
                    format='%(asctime)s %(message)s')

# ---------------------------------------------------------------------------- #
# define graphs
# ---------------------------------------------------------------------------- #
logging.info("read in todes graph and latinx graphs, network metrics, users_df")

# define Cloud Storage bucket
client = storage.Client()
bucket = client.get_bucket('tweethis')

# latinx
blob = bucket.get_blob('processed/latinx_g_exclusive.gpickle')
output_file_name = "latinx_g_exclusive.gpickle"
# download our gpickle blob - do not expand
with open(output_file_name, "wb") as file_obj:
    blob.download_to_file(file_obj, raw_download=True)
# complete user graph is called all_users
latinx_g = nx.read_gpickle(output_file_name)
os.remove(output_file_name)


# todes
blob = bucket.get_blob('processed/todes_g_exclusive.gpickle')
output_file_name = "todes_g_exclusive.gpickle"
# download our gpickle blob - do not expand
with open(output_file_name, "wb") as file_obj:
    blob.download_to_file(file_obj, raw_download=True)
# complete user graph is called all_users
todes_g = nx.read_gpickle(output_file_name)
os.remove(output_file_name)

# preexisting network metrics file
blob = bucket.get_blob('processed/network_metrics.txt')
network_metrics_file = 'network_metrics.txt'
# download the blob
with open(network_metrics_file, 'wb') as file_obj:
    blob.download_to_file(file_obj)

# DATAFRAME OF USERS
users_file = 'network_metrics_by_user_df.pickle'
blob = bucket.get_blob('processed/'+users_file)
with open(users_file, 'wb') as file_obj:
    blob.download_to_file(file_obj, raw_download=True)
users_df = pd.read_pickle(users_file)
os.remove(users_file)

# ---------------------------------------------------------------------------- #
# DEFINE METRICS FILE & ANNOTATE THIS ADDITION
# ---------------------------------------------------------------------------- #
# annotation giving date & filename
sep = "#"*78+"\n\n"
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
annot = "\n\n" + sep + "Metrics added {} from {}.py".format(now, this_file) + \
        "\n\n" + sep

with open(network_metrics_file, 'a') as metrics_file:
    metrics_file.write(annot)

# ---------------------------------------------------------------------------- #
# OVERALL RECIPROCITY
# ---------------------------------------------------------------------------- #
logging.info("calculating overall network reciprocity for each graph")

try:
    todes_reciprocity = nx.algorithms.overall_reciprocity(todes_g)
    latinx_reciprocity = nx.algorithms.overall_reciprocity(latinx_g)

    # write each reciprocity metric
    with open(network_metrics_file, 'a') as metrics_file:
        metrics_file.write("Latinx network overall reciprocity: {} \n\n".format(
            latinx_reciprocity))
        metrics_file.write("Todes network overall reciprocity: {} \n\n".format(
            todes_reciprocity))
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error calculating overall reciprocities")

# ---------------------------------------------------------------------------- #
# NODE LEVEL RECIPROCITY
# ---------------------------------------------------------------------------- #
# todes calc
logging.info("calculating todes node level reciprocity")
try:
    t_reciprocity = nx.algorithms.reciprocity(todes_g, nodes=todes_g.nodes)
    t_reciprocity_series = pd.Series(t_reciprocity, name='t_reciprocity')
    users_df = users_df.join(t_reciprocity_series, how='left')
except (KeyboardInterrupt, SystemExit):
    raise
except:
    logging.exception("error with todes node reciprocity")

#latinx calc
logging.info("calculating latinx node level reciprocity")
try:
    l_reciprocity = nx.algorithms.reciprocity(latinx_g,
                                              nodes=latinx_g.nodes)
    l_reciprocity_series = pd.Series(l_reciprocity, name='l_reciprocity')
    users_df = users_df.join(l_reciprocity_series, how='left')
except (KeyboardInterrupt,SystemExit):
    raise
except:
    logging.exception("error with latinx node reciprocity")

# ---------------------------------------------------------------------------- #
# load network_metrics.txt and network_metrics_by_user dataframe to cloud
# storage
# ---------------------------------------------------------------------------- #
logging.info("saving metrics text file, users df to tweethis/processed")

# NETWORK METRICS.TXT
blob = bucket.blob('processed/'+network_metrics_file)
blob.upload_from_filename(network_metrics_file)
os.remove(network_metrics_file)

# DATAFRAME OF USERS
users_df.to_pickle(users_file)
blob = bucket.blob('processed/'+users_file)
blob.upload_from_filename(users_file)
os.remove(users_file)

logging.info("metrics text file stored. graphs not stored. program terminated.")