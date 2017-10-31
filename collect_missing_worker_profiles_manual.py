'''

Purpose: 
Collects detailed profile information for workers that the user specifies 

'''

import upwork
import httplib2
import oauth2
import urllib3
import json, time, sys
import re
import psycopg2, psycopg2.extras

__author__ = 'Eureka Foong'
__credits__ = 'TBD'
__email__ = 'eurekafoong2020@u.northwestern.edu'

class MissingProfilesManualQuerier: 
    def __init__(self):
        self.log = open('json_files/log_upwork_missing_data_collection_manual_2017_10_31_worldwide_allskills.txt', 'a') # Creating a log
        self.log.write("We have started querying missing profiles!" + "\n")

        # Connect to the database
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

        # Instantiating a new client with access token 
        self.public_key = '956957052307e970d62e6c85e4e52a76'
        self.secret_key = '375a5cc1bbe07402'
        self.oauth_access_token = '835259548ace3959309d17fa02a7143c'
        self.oauth_access_token_secret = '018107b1b8d2bc47'

        self.client = upwork.Client(self.public_key, self.secret_key,
                              oauth_access_token=self.oauth_access_token,
                              oauth_access_token_secret=self.oauth_access_token_secret)
        

    def write_profile(self):

        user_id = input('Enter the worker ID: ')

        try: 
            detailed_info = self.client.provider.get_provider(user_id) # Call the API to return detailed info on each worker 
            user_name = detailed_info["dev_first_name"]
            print "Collected detailed info for " + user_name
            self.cur.execute("INSERT INTO upwork_worldwide_allskills_2017_10_21 (user_id, date_collected, user_name, worker, detailed_info) VALUES (%s, %s, %s, %s, %s);",
                           [user_id, date_collected, user_name, basic_info, psycopg2.extras.Json(detailed_info)])
            
        except psycopg2.IntegrityError: # To prevent duplicate user_id from being added to the database
            self.conn.rollback()
            print "We already have the worker int he database"

        except Exception as err:
            print(err)
            self.log.write("Failed to parse worker: " + user_id + " because of {0}".format(err) + "\n")
            self.log.flush()
            print "Failed to parse worker: " + user_id + "\n"

        else:
            self.conn.commit()
            print "Put detailed info into database"

myObject = MissingProfilesManualQuerier()
myObject.write_profile()