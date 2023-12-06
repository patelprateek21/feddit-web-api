import pandas as pd
import requests
from datetime import datetime

############################################ Pulling Subfeddits Data ###############################################
subfeddit_api_url = "http://localhost:8080/api/v1/subfeddits/?skip=0&limit=100"
subfeddit_response = requests.get(subfeddit_api_url)

data_subfedditsinfo = pd.DataFrame(subfeddit_response.json()['subfeddits'])
print('\nSubfeddits data....\n')
print(data_subfedditsinfo.head())

print('\nSaving raw subfeddit dataset...')
data_subfedditsinfo.to_csv('data/subfedditsinfo_raw.csv',index=False)

###################################### Pulling Comments Data of each Subfeddit ######################################

subfeddits_id = data_subfedditsinfo['id'].tolist()
print('\nSubfeddit ids list....\n')
print(subfeddits_id)

comments_api_url = "http://localhost:8080/api/v1/comments/"

# Pulling in data comments data of each subfeddit subject
data_commentsinfo = pd.DataFrame()
for i in subfeddits_id:
    var_skip = 0
    while True:
        condition = f"?subfeddit_id={i}&skip={var_skip}&limit=5000"
        #print(condition)
        comments_response = requests.get(comments_api_url+condition)
        data = pd.DataFrame(comments_response.json()['comments'])
        data.insert(0,'subfeddit_id',i)
        if len(data)==0:
            break
        data_commentsinfo = pd.concat([data_commentsinfo,data],ignore_index=True)
        var_skip = var_skip+5000

print('\nComments count for each subfeddit....\n')
print(data_commentsinfo.subfeddit_id.value_counts())

print('\nComments data....\n')
print(data_commentsinfo.head(20))

print('\nSaving raw comments dataset...')
data_commentsinfo.to_csv('data/commentsinfo_raw.csv',index=False)