'''

Purpose: 
Collects basic and detailed profile information for workers that were skipped during initial data collection

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

class MissingProfilesQuerier: 
    def __init__(self):
        self.log = open('json_files/log_upwork_missing_data_collection_2017_10_30_worldwide_allskills.txt', 'a') # Creating a log
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

        self.missing_profiles = []
        
    def define_missing_profiles(self):
        with open('json_files/log_upwork_data_collection_2017_10_21_worldwide_allskills.txt') as f:
            for line in f:
                if re.search("Failed to parse worker: ", line):
                    profile_id = line[24:43]
                    self.missing_profiles.append(profile_id)

        print self.missing_profiles

        self.collect_missing_profiles()

    def collect_missing_profiles(self):
        number_of_profiles = 0
        print "We are in collect_missing_profiles"

        print json.dumps (self.client.provider.get_provider("~0160f5925b78a31076"), indent=2)


myObject = MissingProfilesQuerier()
myObject.define_missing_profiles()