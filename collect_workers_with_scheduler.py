import upwork
import httplib2
import oauth2
import urllib3
import json
from apscheduler.schedulers.background import BlockingScheduler
import logging

# Control number of pages, size of page, and time interval
size_of_page = 40
number_of_pages = 1
time_interval = 61 

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

def collect_worker_data():
	if collect_worker_data.offset < number_of_pages + 1:
		print "We're going to start collecting data now!"
		print "The collect_worker_data.offset is"
		print collect_worker_data.offset
		freelancers_on_page = client.provider_v2.search_providers(data=data, page_offset=collect_worker_data.offset, page_size=size_of_page)
		freelancer_basic_data_list.append(freelancers_on_page)
		collect_worker_data.offset += 1

	elif collect_worker_data.offset >= number_of_pages + 1:
		print "collect_worker_data offset done!"
		sched.remove_all_jobs()
		print "We have stopped the first job"
		sched.add_job(collect_detailed_data, 'interval', seconds=time_interval)
		print "We have added the next job"

collect_worker_data.offset = 0

def collect_detailed_data():
	print "Now collecting detailed worker data"
	if collect_detailed_data.freelancer_index < len(freelancer_basic_data_list):
		for i in freelancer_basic_data_list[collect_detailed_data.freelancer_index]:
			print "collecting detailed data at index " + str(collect_detailed_data.freelancer_index)
			freelancer_id = i.get('id')
			freelancer_data = client.provider.get_provider(freelancer_id)
			freelancer_detailed_data_list.append(freelancer_data)
		collect_detailed_data.freelancer_index += 1	

	elif collect_detailed_data.freelancer_index >= len(freelancer_basic_data_list):
		print "collect_detailed_data offset done!"
		with open('10_4_2017_java_developers_3.json', 'w') as f:
			print "Now writing to file!"
			json.dump(freelancer_detailed_data_list, f)
		print "This is all the data we collected"
		print json.dumps(freelancer_detailed_data_list, indent=2)
		sched.remove_all_jobs()
		print "We have stoppped all jobs"

collect_detailed_data.freelancer_index = 0

# Setting up scheduler
sched = BlockingScheduler()
sched.add_job(collect_worker_data, 'interval', seconds=time_interval)


print('Press Ctrl+C to exit')

try:
	sched.start()

except (KeyboardInterrupt, SystemExit):
	pass
