import upwork
import httplib2
import oauth2
import urllib3
import json

public_key = '956957052307e970d62e6c85e4e52a76'
secret_key = '375a5cc1bbe07402'
oauth_access_token = '835259548ace3959309d17fa02a7143c'
oauth_access_token_secret = '018107b1b8d2bc47'

# Instantiating a new client with access token 
client = upwork.Client(public_key, secret_key,
                      oauth_access_token=oauth_access_token,
                      oauth_access_token_secret=oauth_access_token_secret)

# Searching for freelancers
data = {'q': 'united states'}
count = 0
job_list = client.provider_v2.search_jobs(data=data, page_offset=0, page_size=2)

for i in job_list:
	count += 1

with open('jobData.json', 'w') as f:
	json.dump(job_list, f)
