################################################################################
# Split our complete graph into either latinx or todes corpus (exclusive).
# For each exclusive graph, find;
# - Network information
# - Average cluster coefficient of the network
# - Density
# - Triad census
#
# Inputs
# ------
# tweethis/processed/todes_g_exclusive.gpickle
# tweethis/processed/latinx_g_exclusive.gpickle
#
# Outputs
# -------
# tweethis/processed/network_metrics.txt
################################################################################
import networkx as nx
import logging
import os
from google.cloud import storage
from datetime import datetime
from networkx.algorithms import approximation as appx

logging.basicConfig(filename='network_metrics.log', level=logging.INFO,
                    format='%(asctime)s %(message)s')

this_file = "network_metrics"

# ---------------------------------------------------------------------------- #
# define graphs
# ---------------------------------------------------------------------------- #
logging.info("read in todes graph and latinx graph")

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

# ---------------------------------------------------------------------------- #
# DEFINE METRICS FILE and give basic graph information for each corpus
# ---------------------------------------------------------------------------- #
# annotation giving date & filename
sep = "#"*78+"\n\n"
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
annot = sep + "Metrics added {} from {}.py".format(now, this_file) + \
        "\n\n" + sep

with open("network_metrics.txt", 'a') as metrics_file:
    metrics_file.write(annot)
    metrics_file.write(nx.info(latinx_g)+"\n\n")
    metrics_file.write(nx.info(todes_g) + "\n\n")


# ---------------------------------------------------------------------------- #
# AVG CLUSTER COEFFICIENT
# ---------------------------------------------------------------------------- #
logging.info("calculating cluster coeff for each network")
todes_gu = nx.to_undirected(todes_g)
latinx_gu = nx.to_undirected(latinx_g)

t_cluster_coeff = appx.average_clustering(todes_gu, trials=10000, seed=115)
l_cluster_coeff = appx.average_clustering(latinx_gu, trials=10000, seed=115)

# write each cluster coefficient
with open("network_metrics.txt", 'a') as metrics_file:
    metrics_file.write("Latinx network average cluster coeff: {} \n\n".format(
        l_cluster_coeff))
    metrics_file.write("Todes network average cluster coeff: {} \n\n".format(
        t_cluster_coeff))


# ---------------------------------------------------------------------------- #
# NETWORK DENSITY
# ---------------------------------------------------------------------------- #
logging.info("calculating network density")
with open("network_metrics.txt", 'a') as metrics_file:
    metrics_file.write("Latinx Density: {}\n\n".format(nx.density(latinx_g)))
    metrics_file.write("Todes Density: {}\n\n".format(nx.density(todes_g)))


# ---------------------------------------------------------------------------- #
# TRIADIC CENSUS
# ---------------------------------------------------------------------------- #
logging.info("calculating triadic census for latinx & todes")
lx_triad_census = nx.triadic_census(latinx_g)
te_triad_census = nx.triadic_census(todes_g)

with open("network_metrics.txt", 'a') as metrics_file:
    metrics_file.write("Latinx Triadic Census:\n")
    for k, v in lx_triad_census.items():
        metrics_file.write((str(k) + ' : '+ str(v) + "\n"))
    metrics_file.write("\n\n")

    metrics_file.write("Todes Triadic Census:\n")
    for k, v in te_triad_census.items():
        metrics_file.write((str(k) + ' : '+ str(v) + "\n"))
    metrics_file.write("\n\n")


# ---------------------------------------------------------------------------- #
# DUMP network_metrics.txt to GCP Cloud Storage
# tweethis/processed/network_metrics.txt
# ---------------------------------------------------------------------------- #
logging.info("saving metrics text file to cloud storage")

# define local file name
this_file_out = this_file+'.txt'
# define the blob
blob = bucket.blob('processed/'+this_file_out)
# upload
blob.upload_from_filename(this_file_out)
os.remove(this_file_out)

logging.info("metrics text file stored. graphs not stored. program terminated.")