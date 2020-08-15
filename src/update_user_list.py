################################################################################
# This script was used to update the list of users when
# get_following_list_per_user.py was interrupted. It removes the users that
# could not be pinged as well as the users already pinged from the unqueried
# users.
#
# Input:
# repo/data/processed/user_following/user_list.pkl
# Output:
# repo/data/processed/user_following/user_list.pkl
################################################################################
import pickle
import re


################################################################################
################################################################################
# ----- Update this each run ----- #
this_addition = "../data/processed/user_following/saved_users4.txt"
this_subtraction = "../data/processed/user_following/following_list_log4.txt"
################################################################################
################################################################################


# ----- completed user ids ----- #
completed_source_users = []

with open(this_addition, "r") as f:
    for line in f:
        process = line.split(" ", 1)
        source = process.pop(0)
        completed_source_users.append(source)

# ----- rejected user ids ----- #
rejected_users = []
pattern = re.compile("user (\d+) and")

with open(this_subtraction, "r") as log:
    for line in log.readlines():
        result = pattern.search(line)
        if result:
            rejected_users.append(line[result.start()+5:result.end()-4])


print("completed users "+str(len(completed_source_users)))
print("rejected users "+str(len(rejected_users)))


# now remove completed & rejected user id's from query list
with open("../data/processed/user_following/user_list.pkl", 'rb') as picklefile:
     prev_user_list = pickle.load(picklefile)
user_list = [item for item in prev_user_list if item not in
          completed_source_users]
user_list = [item for item in user_list if item not in rejected_users]
with open('../data/processed/user_following/user_list.pkl', 'wb') as picklefile:
    pickle.dump(user_list, picklefile)
