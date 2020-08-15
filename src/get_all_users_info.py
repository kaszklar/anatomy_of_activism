################################################################################
# This script runs through a list of strings of user names and creates a
# pandas dataframe of all users in the dataset, pulling from the twitter
# users/lookup api. For each user we scrape
# `['id_str', 'name', 'screen_name', 'location',
#                                 'description', 'url','entities', 'protected',
#                                 'followers_count', 'friends_count',
#                                 'listed_count','created_at','verified',
#                                 'statuses_count','lang']`
# Input:
# repo/data/raw/LX-Sept2019.csv
# repo/data/raw/TE-Sept2019.csv
# Output:
# repo/data/processed/combo_user_df_sept19.pkl
################################################################################
import requests
from requests_oauthlib import OAuth1
import cnfg
import pandas as pd
import pickle
import numpy as np


def process_screennames(arr):
    """
    Turn an array of user names into a list of lists of usernames in which
    each list of usernames contains no more than 100 usernames.

    :param arr: 1D array of usernames
    :return: 2D python list of usernames
    """
    if arr.size % 100 > 0:
        # get the next integer up when dividing by 100
        next_int = int((arr.size / 100) + 1)

    # get the number of null strings we need
    num_new_elements = abs(arr.size % 100 - 100)

    # appending enough null strings to reach a round number divisible by 100
    temp = np.append(arr, np.full(num_new_elements, ""))

    # reshape the array into n,100 sized
    array_batches = np.reshape(temp, (next_int, 100))

    # convert to a python list, in the process parse the strings so that
    # they are separated by , but no spaces
    # this is to append to the twitter API request
    list_batches = []
    for batch in array_batches:
        s = ""
        for screenname in batch:
            s = s + screenname + ","
        list_batches.append(s)

    # strip trailing ,
    list_batches = [x.strip(",") for x in list_batches]

    return list_batches


def call_twitter(list_batches, latinx=True):
    """
    Call the twitter users/lookup API using 100 screen_names at a time. Save
    results to a python dataframe.

    :param list_batches: 2D python list of usernames, each list is 100 usernames
    long
    :param latinx: Boolean. True if the batch of usernames being processed is
    the latinx corpus, false if todes coprus
    :return:
    """
    global df

    for i, names in enumerate(list_batches):
        endpoint = "https://api.twitter.com/1.1/users/lookup.json?screen_name" \
                   "=" + names

        # API call & turn into json
        response = requests.get(endpoint, auth=oauth)
        users = response.json()

        # iterate thru the 100 users returned, and append to the big ol df
        for u in range(len(users)):
            try:
                d = {el: users[u][el] for el in columns}
                d['latinx'] = 1 if latinx else 0
                d['todes'] = 0 if latinx else 1
                df = df.append(d, ignore_index=True)
            except Exception as ex:
                print(ex)
                print("exception at batch " + str(i))
                print("exception at u " + str(u))
                raise  # re-raise exception - interrupt


# ---------------------------------------------------------------------------- #
# Authorize twitter API
# ---------------------------------------------------------------------------- #
config = cnfg.load("/Users/katie/.twitter_config")

oauth = OAuth1(config["consumer_key"],
               config["consumer_secret"],
               config["access_token"],
               config["access_token_secret"])


# ---------------------------------------------------------------------------- #
# Create a big ol' dataframe to start
# ---------------------------------------------------------------------------- #
columns = ['id_str', 'name', 'screen_name', 'location', 'description', 'url',
           'entities', 'protected', 'followers_count', 'friends_count',
           'listed_count', 'created_at','verified', 'statuses_count','lang']

global df
df = pd.DataFrame(columns=columns.copy().extend(['latinx','todes']))

# ---------------------------------------------------------------------------- #
# Pull in raw tweet data
# ---------------------------------------------------------------------------- #
latinx_tweets = pd.read_csv("../data/raw/LX-Sept2019.csv")
todes_tweets = pd.read_csv("../data/raw/TE-Sept2019.csv")

# ---------------------------------------------------------------------------- #
# Get list of unique user names
# ---------------------------------------------------------------------------- #
latinx_screennames = latinx_tweets.username.unique()
todes_screennames = todes_tweets.username.unique()

# ---------------------------------------------------------------------------- #
# Turn those usernames into appropriately sized batches for the api
# ---------------------------------------------------------------------------- #
latinx_batches = process_screennames(latinx_screennames)
todes_batches = process_screennames(todes_screennames)

# ---------------------------------------------------------------------------- #
# Ping API for each corpus
# ---------------------------------------------------------------------------- #
call_twitter(latinx_batches, latinx=True)
call_twitter(todes_batches, latinx=False)

# ---------------------------------------------------------------------------- #
# Ensure that users in both corpora are noted as such
# ---------------------------------------------------------------------------- #
# these users are in both corpora
double_users = list(set(todes_tweets['username']).
                    intersection(set(latinx_tweets['username'])))
df.set_index('screen_name', inplace=True)

for user in double_users:
    combo.at[user,'todes'] = 1
    combo.at[user,'latinx'] = 1

df.reset_index(inplace=True)
df.drop_duplicates('id_str', inplace=True)

# ---------------------------------------------------------------------------- #
# Save out the df as a serialized obj for later
# ---------------------------------------------------------------------------- #
with open('../data/processed/combo_user_df_sept19.pkl', 'wb') as picklefile:
    pickle.dump(df, picklefile)
