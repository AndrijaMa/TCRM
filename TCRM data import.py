##**************************************************************************************************************************************##
##This code does not contain error handling or logging functionality and is not intended for production use. Please use with caution ***##
##**************************************************************************************************************************************##

import requests
import pandas as pd
import base64

params = {
    "grant_type": "password",
    "client_id": "**--**", # Consumer Key
    "client_secret": "**--**", # Consumer Secret
    "username": "**--**", # The email you use to login
    "password": "**--**" # Concat your password and your security token
}

Dataset_Import_Name = '****'

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


#Read local CSV and encode with base64

with open('SparNord.csv', 'rb') as binary_file:
    binary_file_data = binary_file.read()
    base64_encoded_data = base64.b64encode(binary_file_data)
    base64_message = base64_encoded_data.decode('utf-8')

#Initialize file upload
call = sf_api_call("/services/data/v53.0/sobjects/InsightsExternalData", method="post", data={
 'Format' : 'csv',
 'EdgemartAlias' : Dataset_Import_Name,
 'Operation' : 'Overwrite',
 'Action' : 'None'
})
id = call['id']

call = sf_api_call("/services/data/v53.0/sobjects/InsightsExternalDataPart", method="post", data={
  'DataFile' : base64_message,
  'InsightsExternalDataId' : id,
  'PartNumber': 1
})

#Start the upload
call = sf_api_call("/services/data/v53.0/sobjects/InsightsExternalData/"+id, method="patch", data={
    'Action': 'Process' 
})