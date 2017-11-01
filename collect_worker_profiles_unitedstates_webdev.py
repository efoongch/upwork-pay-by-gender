'''

Purpose: 
Collects basic and detailed profile information on workers in the U.S. and stores in PostgreSQL database 

'''

import upwork
import httplib2
import oauth2
import urllib3
import json, time, sys
import logging
import psycopg2, psycopg2.extras

__author__ = 'Eureka Foong'
__credits__ = 'TBD'
__email__ = 'eurekafoong2020@u.northwestern.edu'

class UpworkQuerier: 
    def __init__(self):
        self.log = open('json_files/log_upwork_data_collection_2017_11_01_unitedstates_webdev.txt', 'a') # Creating a log
        self.log.write("We have started collecting data!" + "\n")

        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

        # Instantiating a new client with access token 
        self.public_key = 'ed8ff5784819b4dbadb800d8e9e10e21'
        self.secret_key = '03a6e678f4ee3246'
        self.oauth_access_token = 'b60900600fc9e6da57d7711c2d098591'
        self.oauth_access_token_secret = '5022f67f9bccb73d'

        self.client = upwork.Client(self.public_key, self.secret_key,
                              oauth_access_token=self.oauth_access_token,
                              oauth_access_token_secret=self.oauth_access_token_secret)
        
    def collect_workers_basic_data(self): 
        self.size_of_page = 8 # Number of profiles you want to collect at once (set 8 or less to collect about 500 per hour) 
        self.offset = 0 # Starting offset 
        self.data = {'q': 'united states', 'category2': 'Web, Mobile & Software Dev'} # Parameters for searching for freelancers (use 'q': 'a' if want to search all freelancers)

        while True:
            print "Now about to take profiles at offset {0}".format(self.offset)
            try: 
                print "Now about to sleep before we collect basic info again"
                time.sleep(61)
                freelancers_on_page = self.client.provider_v2.search_providers(data=self.data, page_offset=self.offset, page_size=self.size_of_page)
                
            except Exception as err: # Prevents errors due to faulty API call (400 error)
                print (err) 
                print "Failed to collect data at offset {0}".format(self.offset)
                self.log.write("Failed to collect data at offset {0}".format(self.offset) + "because of {0}".format(err) + "\n")
                self.log.flush()
                self.offset += 1

            except KeyboardInterrupt: 
                self.log.write("Interrupted at offset {0}".format(self.offset))
                print "Interrupted at offset {0}".format(self.offset)
                break

            else:
                print "Now about to sleep before we collect detailed profile info"
                time.sleep(61)
                print "Collecting detailed profile info and storing into database"
                self.on_workers(freelancers_on_page)
                self.offset += self.size_of_page

    def on_workers(self, workers):
        for worker in workers:
            user_id = worker["id"]
            user_name = worker["name"]
            date_collected = "11_01_2017"
            
            try:
                time.sleep(1.5) # To prevent nonce error, make sure two requests aren't being sent at the same second  
                detailed_info = self.client.provider.get_provider(user_id) # Call the API to return detailed info on each worker 
                self.cur.execute("INSERT INTO upwork_unitedstates_webdev_2017_11_01 (user_id, date_collected, user_name, worker, detailed_info) VALUES (%s, %s, %s, %s, %s);",
                                [user_id, date_collected, user_name, psycopg2.extras.Json(worker), psycopg2.extras.Json(detailed_info)])

            except psycopg2.IntegrityError: # To prevent duplicate user_id from being added to the database
                self.conn.rollback()

            except Exception as err: # Any other errors, simply mark down the user_id and we will collect data later
                print(err)
                self.log.write("Failed to parse worker: " + user_id + " because of {0}".format(err) + "\n")
                self.log.flush()
                print "Failed to parse worker: " + user_id + "\n"

            else:
                self.conn.commit()

myObject = UpworkQuerier()
myObject.collect_workers_basic_data()