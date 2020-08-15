################################################################################
# This script splits the dataframe of network metrics @ the user level into
# separate todes and latinx files.
#
# Input:
# repo/data/processed/user_following/processed_network_metrics_by_user_df.pickle
#
# Outputs:
# repo/data/final/todes_exclusive_users_metrics_df.pickle
# repo/data/final/latinx_exclusive_users_metrics_df.pickle
################################################################################

import pandas as pd

df = pd.read_pickle("../data/processed/user_following"
                    "/processed_network_metrics_by_user_df.pickle")

# ---------------------------------------------------------------------------- #
# Define columns to be rejected
# ---------------------------------------------------------------------------- #
todes_cols = df.columns.difference(['l_bet_central','l_out_deg_central',
                                    'l_in_deg_central', 'l_in_2hop',
                                    'l_out_2hop', 'l_deg_central', 'l_out_deg',
                                    'l_in_deg', 'l_clustering',
                                    'l_reciprocity'])

latinx_cols = df.columns.difference(['t_clustering', 't_in_deg', 't_out_deg',
                                     't_deg_central','t_out_2hop',
                                     't_in_2hop', 't_in_deg_central',
                                     't_out_deg_central', 't_bet_central',
                                     't_reciprocity'])

# ---------------------------------------------------------------------------- #
# Split dataframes based on the column differences
# ---------------------------------------------------------------------------- #
todes = df[df.corpus!='latinx'][todes_cols]
latinx = df[df.corpus!='todes'][latinx_cols]

# ---------------------------------------------------------------------------- #
# Rename columns; remove leading "t_" and "l_"
# ---------------------------------------------------------------------------- #
for col in todes.columns:
    try:
        new = str(col).lstrip("t_")
        todes.rename(columns={col: new}, inplace=True)
    except:
        pass

for col in latinx.columns:
    try:
        new = str(col).lstrip("l_")
        latinx.rename(columns={col: new}, inplace=True)
    except:
        pass

# ---------------------------------------------------------------------------- #
# Save outputs
# ---------------------------------------------------------------------------- #
todes.to_pickle("../data/final/todes_exclusive_users_metrics_df.pickle")
latinx.to_pickle("../data/final/latinx_exclusive_users_metrics_df.pickle")