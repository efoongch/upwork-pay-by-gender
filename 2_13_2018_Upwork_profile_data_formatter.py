import httplib2
import oauth2
import urllib3
import types
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from gender_detector import GenderDetector 
import psycopg2, psycopg2.extras
from causalinference import CausalModel
from causalinference.utils import random_data
import httplib
import base64
import json # For Microsoft Face API
import urllib as urllib # For Microsoft Face API
import time 
import csv
import datetime 


class UpworkDataFormatter:
    
    def __init__(self):
        # Settings
        self.all_data_file_name = './csv_files/profile_data2_2017_12_12_upwork_analysis_unitedstates_allskills.csv' # Filename for all data
        self.data_log_file_name = './log_files/profile_data2_log_upwork_data_analysis_2017_12_12_unitedstates_allskills.txt'
        
        # Write a log
        self.log = open(self.data_log_file_name, 'a')
        self.log.write("We have started analyzing data!" + "\n")
        self.log.flush()

        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
        
        # Get detailed_info from workers in our database
        self.cur.execute("SELECT detailed_info FROM upwork_unitedstates_allskills_2017_12_12;")
        
        # Start counting users 
        self.user_count = 1
        self.job_category_id_list = []
    
    def save_all_to_csv(self):
        with open (self.all_data_file_name, 'w') as csvfile:
            fieldnames = ['user_count','worker_id', 'bill_rate', 'job_category', 'job_category_id',
                'jobs_completed', 'profile_desc', 'hours_worked', 'feedback_score', 'no_reviews', 'eng_skill', 'agency']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for user in self.cur:
                try: 
                    user_count = self.user_count
                    worker_id = user[0]["ciphertext"]
                    bill_rate = user[0]["dev_bill_rate"]
                    job_category = self.identify_job_category(user)
                    job_category_id = self.encode_category(job_category, self.job_category_id_list)
                    jobs_completed = user[0]['dev_billed_assignments'] # Number of billed assignments 
                    profile_desc = user[0]['dev_blurb'].encode("utf-8").strip()
                    hours_worked = user[0]['dev_total_hours']
                    feedback_score = user[0]['dev_adj_score']
                    no_reviews = user[0]['dev_tot_feedback']
                    eng_skill = user[0]['dev_eng_skill']
                    agency = user[0]['dev_is_affiliated']
                    
                    writer.writerow({'user_count': self.user_count, 'worker_id': worker_id, 'bill_rate': bill_rate, 
                        'job_category': job_category, 'job_category_id': job_category_id, 'jobs_completed': jobs_completed, 
                        'profile_desc': profile_desc, 'hours_worked': hours_worked, 'feedback_score': feedback_score, 
                        'no_reviews': no_reviews, 'eng_skill': eng_skill, 'agency': agency})
                    
                except KeyboardInterrupt:
                    print "We got interrupted"
                    break
                    
                except Exception as error:
                    print "!!!!!Ran into some error at user {0}".format(self.user_count)
                    print error
                    
                    writer.writerow({'user_count': self.user_count, 'worker_id': "error", 'bill_rate': "error", 
                        'job_category': "error", 'job_category_id': "error", 'jobs_completed': "error", 
                        'profile_desc': "error", 'hours_worked': "error", 'feedback_score': "error", 
                        'no_reviews': "error", 'eng_skill': "error", 'agency': "error"})
                    
                print "Finished writing data for {0}".format(self.user_count)
                self.user_count += 1
    
    def encode_category(self, item, id_list):
        if not(item in id_list):
            id_list.append(item)
        
        category_index = id_list.index(item)
        return category_index
    
    def identify_job_category(self, user):
        try:
            all_job_categories = user[0]["dev_job_categories_v2"]["dev_job_categories_v"]
            
        except:
            job_category = "none"
            return job_category
        
        try:
            job_category = all_job_categories[0]["groups"]["group"]["name"] # Returns general job category
        
        except:
            job_category = all_job_categories["groups"]["group"]["name"]
        
        return job_category

myObject = UpworkDataFormatter()
myObject.save_all_to_csv()