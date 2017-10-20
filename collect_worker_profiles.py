'''
collect_worker_profiles.py

Purpose: 
Collects basic information on a set number of worker profiles and store in PostgreSQL database 

Left to do:
- Detecting duplicates and not storing those in the database as additional rows
- (If possible?) Not collecting worker profiles that we have already seen 

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

#logging.basicConfig()
class UpworkQuerier: 
    def __init__(self):
    
        # Creating a log of what happened during session
        self.log = open('json_files/log_upwork_data_collection_2017_10_20.txt', 'a')
        self.log.write("We have started collecting data")
        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

        # Specify when to commit changes
        self.commiton = 20000
        self.counter = 0

        # Instantiating a new client with access token 
        self.public_key = '956957052307e970d62e6c85e4e52a76'
        self.secret_key = '375a5cc1bbe07402'
        self.oauth_access_token = '835259548ace3959309d17fa02a7143c'
        self.oauth_access_token_secret = '018107b1b8d2bc47'

        self.client = upwork.Client(self.public_key, self.secret_key,
                              oauth_access_token=self.oauth_access_token,
                              oauth_access_token_secret=self.oauth_access_token_secret)
        
    def collect_workers_basic_data(self): 
        # Control number of users you want to collect at once 
        self.size_of_page = 8
        self.offset = 0

        # Parameters for searching for freelancers
        self.data = {'q': 'a'}

        # While we have collected fewer than x profiles, collect 40 profiles at a time and add that to a list
        while self.offset < 16:
            print "Now about to take profiles at offset {0}".format(self.offset)
            freelancers_on_page = self.client.provider_v2.search_providers(data=self.data, page_offset=self.offset, page_size=self.size_of_page)
            print "Now about to sleep before we collect detailed profile info"
            time.sleep(61)
            print "Collecting detailed profile info and storing into database"
            self.on_workers(freelancers_on_page)
            self.offset += self.size_of_page
            print "Now about to sleep before we collect basic info again"
            time.sleep(61)

    def on_workers(self, workers):

        # Strip the list into individual workers, then save workers as different rows in our database
        for worker in workers:
            try:
                user_id = worker["id"]
                first_name = worker["name"].split(' ', 1)[0]
                profile_photo = "10"
                # Call the API to return detailed info on each worker 
                detailed_info = self.client.provider.get_provider(user_id)
                self.cur.execute("INSERT INTO general_workers_as_json_2017_10_20 (user_id, first_name, profile_photo, worker, detailed_info) VALUES (%s, %s, %s. %s, %s);",
                                [user_id, first_name, profile_photo, psycopg2.extras.Json(worker), psycopg2.extras.Json(detailed_info)])

            except psycopg2.IntegrityError:
                self.conn.rollback()

            except Exception as err:
                print(err)
                # If something goes wrong, still save the worker data into the table
                self.cur.execute("INSERT INTO general_workers_as_json_2017_10_20 (worker) VALUES (%s);", [psycopg2.extras.Json(worker)])
                self.log.write("Failed to parse worker: " + user_id + "\n")
                self.log.flush()
                print "Failed to parse worker: " + user_id + "\n"

            else:
                self.conn.commit()

        '''
        # Start counting how many pages of workers we have added to the table
        self.counter += 1
        
        # If we have collected a lot of workers, commit the changes to the database
        if self.counter >= self.commiton:
            self.conn.commit()
            self.log.write("Committed " + str(self.counter) + ": " + time.strftime('%Y%m%d-%H%M%S') + "\n")
            self.log.flush()
            print "Committed" + str(self.counter) + ": " + time.strftime('%Y%m%d-%H%M%S') + "\n"
            self.counter = 0
        return
        '''


# Call function to collect basic data on workers
myObject = UpworkQuerier()
myObject.collect_workers_basic_data()

