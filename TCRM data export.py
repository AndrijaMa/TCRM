##**************************************************************************************************************************************##
##This code does not contain error handling or logging functionality and is not intended for production use. Please use with caution ***##
##**************************************************************************************************************************************##

import requests
import json
import time
import pandas as pd


params = {
    "grant_type": "password",
    "client_id": "**--**", # Consumer Key
    "client_secret": "**--**", # Consumer Secret
    "username": "**--**", # The email you use to login
    "password": "**--**" # Concat your password and your security token
}

tcrm_dataset_name = "****" #The name of the TCRM dataset that you want to export
timestr = time.strftime("%Y%m%d-%H%M%S")+".csv"

r = requests.post("https://login.salesforce.com/services/oauth2/token", params=params)
# if you connect to a Sandbox, use test.salesforce.com instead
access_token = r.json().get("access_token")
instance_url = r.json().get("instance_url")

def sf_api_call(action, parameters = {}, method = 'get', data = {}):
    """
    Helper function to make calls to Salesforce REST API.
    Parameters: action (the URL), URL params, method (get, post or patch), data for POST/PATCH.
    """
    headers = {
        'Content-type': 'application/json',
        'Accept-Encoding': 'gzip',
        'Authorization': 'Bearer %s' % access_token
    }
    if method == 'get':
        r = requests.request(method, instance_url+action, headers=headers, params=parameters, timeout=30)
    elif method in ['post', 'patch']:
        r = requests.request(method, instance_url+action, headers=headers, json=data, params=parameters, timeout=10)
    elif method in ['delete']:
        r = requests.request(method, instance_url+action, headers=headers, params=parameters, timeout=10)  
    else:
        # other methods not implemented in this example
        raise ValueError('Method should be get or post or patch.')
    print('Debug: API %s call: %s %s' % (method, r.url,r.text) )
    if r.status_code < 300:
        if method=='patch':
            return None
        else:
            return r.json()
    else:
        raise Exception('API error when calling %s : %s' % (r.url, r.content))


#Get all datasets
datasets = sf_api_call('/services/data/v53.0/wave/datasets/', method="get")

#Filter down to the dataset that you want to export
d = [x for x in datasets['datasets'] if x['label'] == tcrm_dataset_name]

#Get the id and current version id for the dataset that you want to download
dataid = d[0]['id']
dataversionid = d[0]['currentVersionId']

#Initialize dataet download
call = sf_api_call('/services/data/v53.0/wave/query/', method="post", data={
'query':  "q = load \""+dataid+"/"+dataversionid+"\"; result =foreach q generate AccountNumber as AccountNumber, Id as Id, Name as Name"  
})

#
d = pd.read_json(json.dumps(call['results']['records']))
#Create Pandas dataset from the downloaded dataset
datafile = pd.DataFrame(d)

#Save to a local file
datafile.to_csv(timestr)