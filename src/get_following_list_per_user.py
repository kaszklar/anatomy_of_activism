################################################################################
# This script runs through a list of twitter user names and queries the
# twitter friends/ids endpoint in order to get a list of ids that they
# follow. This is saved in a .txt file. Each line beings with the user in
# question followed by ids separated as spaces.
#
# Input:
# repo/data/processed/user_following/user_list.pkl
# Output:
# repo/data/processed/user_following/*
################################################################################
import requests
from requests_oauthlib import OAuth1
import cnfg
import pickle
import traceback
import time
from datetime import date, datetime

# get twitter credentials confidentially
config = cnfg.load(".twitter_config")
oauth = OAuth1(config["consumer_key"],
               config["consumer_secret"],
               config["access_token"],
               config["access_token_secret"])

with open("../data/processed/user_following/user_list.pkl", 'rb') as picklefile:
     user_list = pickle.load(picklefile)

while len(user_list) > 0:
    user = user_list.pop(0)
    try:
        response = requests.get(
            "https://api.twitter.com/1.1/friends/ids.json?user_id="+user,
            auth=oauth)
        cursor = '0'
        status = response.status_code

        if status != 200:  # making sure we are within rate limits
            time.sleep(63)

        response_json = response.json()
        ids = response_json['ids']

        # stringify list of ids
        list_of_strings = [str(u) for u in ids]

        long_s = ""

        for s in list_of_strings:
            long_s = long_s + " " + s

        while response_json['next_cursor'] > 0:
            cursor = response_json['next_cursor_str']

            # API call & turn into json
            req = "https://api.twitter.com/1.1/friends/ids.json?user_id" \
                  "=" + user + '&cursor=' + cursor

            response = requests.get(req, auth=oauth)

            if status != 200:  # just in case
                time.sleep(63)

            response_json = response.json()
            ids = response_json['ids']

            # stringify the current list of following ids
            list_of_strings = [str(u) for u in ids]

            # and append to long string
            for s in list_of_strings:
                long_s = long_s + " " + s

        # append to the .txt file the long string, beginning with the user id
        with open("../data/processed/user_following/saved_users4.txt",
                  "a") as following_list:
            following_list.write(user + " " + long_s + "\n")

    except Exception as ex:
        now = date.strftime(datetime.now(), format='%Y-%m-%d %H:%M')
        status_str = str(status)
        with open("../data/processed/user_following/following_list_log4.txt",
                  "a") as log:
            log.write(now + " @ user " + str(user) + "and cursor " + cursor +
                      " and response code " + status_str + "\n")
            traceback.print_exc(limit=None, file=log, chain=True)
    finally:
        time.sleep(63)
