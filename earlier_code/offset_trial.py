import upwork
import httplib2
import oauth2
import urllib3
import json
import logging
import time

# Control number of pages, size of page, and time interval
size_of_page = 40
offset = 0
user_number = 1

logging.basicConfig()

public_key = '956957052307e970d62e6c85e4e52a76'
secret_key = '375a5cc1bbe07402'
oauth_access_token = '835259548ace3959309d17fa02a7143c'
oauth_access_token_secret = '018107b1b8d2bc47'

# Instantiating a new client with access token 
client = upwork.Client(public_key, secret_key,
                      oauth_access_token=oauth_access_token,
                      oauth_access_token_secret=oauth_access_token_secret)

# Searching for freelancers
data = {'q': 'united states', 'skills' : 'java'}
freelancer_basic_data_list = []
freelancer_detailed_data_list = []

# While loop to collect basic data and add to a list
while offset < 500:
	print "Now about to take profiles at offset {0}".format(offset)
	freelancers_on_page = client.provider_v2.search_providers(data=data, page_offset=offset, page_size=size_of_page)
	freelancer_basic_data_list.append(freelancers_on_page)
	offset += size_of_page
	print "Now about to sleep"
	time.sleep(61)

with open('10_15_2017_java_developers_500_1.json', 'w') as f:
			print "Now writing to file!"
			json.dump(freelancer_basic_data_list, f)

'''
# Prints out the user's name 
for page in freelancer_basic_data_list:
	for user in page:
		user_name = user["name"]
		print "User at {0} is ".format(user_number) + user_name
		user_number += 1
'''



