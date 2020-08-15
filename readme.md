# Anatomy of Activism

A comparative network analysis of two different activist groups on twitter. How do they use the network? Over 50k users and 100k relationships were mapped using GCP bigquery, compute, and cloud storage services. 

At the moment, this repo contains the scripts used to pull relationship information from the twitter user/lookup and friends/ids API endpoints and build out a following graph. 

Order of execution:

1. **get_all_users_info.py** 
   Reading the raw tweet csv's, create a dataframe of user information. Ping the users/lookup endpoint to pull info such as id, # of friends, followers, tweets, account description, name, whether protected, verified, listed, and when account joined twitter. 
2. **process_users_corpora.py**
   Confirm corpus assignment was done correctly. 
3.  **get_following_list_per_user.py** 
   Following API rate limits (1 request/minute), generate following list of each user in our network into .txt files. 🚨 This will take approximately 5 weeks to run. 🚨 If interrupted, run **update_user_list.py**.
4. **user_following_graph.py**
   Using the .txt files, generate a super digraph of following relationships for both corpora of users using [networkx](https://networkx.github.io/). Also create a subgraph for each corpus. Save graphs to gcp cloud storage.
5. **network_metrics_by_user.py**
   Generate a dataframe of users & their clustering coefficient, in & out degree centrality, betweenness centrality, # of predecessors & successors in the alternative corpus (this analysis excludes users that appear in both corpora).
6. **reciprocity.py**
   Assign each user in the network metrics df their reciprocity score. This metric was originally left out of **network_metrics_by_user.py**.
7. **network_metrics.py**
   Generate a text file of graph information for each follow graph; number of nodes, edges, avg in & out degrees, density, triadic census. Output to a text log.
8.  **finalize_exclusive_metrics_by_user.py**
   Separate the full dataframe of user metrics by corpus for ease of analysis.