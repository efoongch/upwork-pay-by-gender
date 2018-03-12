import httplib2
import oauth2
import urllib3
import types
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from gender_detector import GenderDetector 
from genderComputer import GenderComputer
import sexmachine.detector as gender 
import gender_guesser.detector as gender_guesser
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
        self.all_data_file_name = './csv_files/altgender4_2017_12_12_upwork_analysis_unitedstates_allskills.csv' # Filename for all data
        self.data_log_file_name = './log_files/alt_gender4_log_upwork_data_analysis_2017_12_12_unitedstates_allskills.txt'
        
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
        
        # Initialize arrays for Causal Analysis 
        self.user_count = 1

        # Initialize gender detectors
        self.d = gender.Detector()
        self.gc = GenderComputer('./nameLists') 
        self.us_detector = GenderDetector('us')
        self.ar_detector = GenderDetector('ar')
        self.uk_detector = GenderDetector('uk')
        self.uy_detector = GenderDetector('uy')
        self.gender_guesser = gender_guesser.Detector()

    
    def save_all_to_csv(self):
        with open(self.all_data_file_name, 'w') as csvfile:
            fieldnames = ['user_count','worker_id', 'first_name', 'profile_desc', 
            'gender_guesser', 'gender_detector', 'sex_machine', 'gender_computer',
            'gender_pronoun']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for user in self.cur:
                try: 
                    user_count = self.user_count
                    worker_id = user[0]["ciphertext"]
                    first_name = user[0]["dev_first_name"].encode('utf-8').strip()
                    print "Successfully done first_name"
                    profile_desc = user[0]['dev_blurb'].encode('utf-8').strip()
                    gender_guesser = self.gender_by_guesser(first_name) # Output of gender guesser
                    gender_detector = self.gender_by_detector(first_name) # Output of gender detector
                    sex_machine = self.gender_by_sex_machine(first_name) # Output of sex machine
                    gender_computer = self.gender_by_computer(first_name) # Output of gender computer
                    gender_pronoun = self.gender_by_pronoun(user) # Output of pronoun in reviews
                    
                    writer.writerow({'user_count': self.user_count, 'worker_id': worker_id, 'first_name': first_name,
                        'profile_desc': profile_desc, 'gender_guesser': gender_guesser, 'gender_detector': gender_detector, 
                        'sex_machine': sex_machine, 'gender_computer': gender_computer, 'gender_pronoun': gender_pronoun})
                
                except KeyboardInterrupt:
                    print "We got interrupted"
                    break 
                    
                except Exception as error:
                    print "Ran into some error at user {0}".format(self.user_count)
                    print error
                    
                    writer.writerow({'user_count': self.user_count, 'worker_id': "error", 'first_name': "error",
                        'profile_desc': "error", 'gender_guesser': "error", 'gender_detector': "error", 
                        'sex_machine': "error", 'gender_computer': "error", 'gender_pronoun': "error"})
                    
                print "Finished writing data for {0}".format(self.user_count)
                self.user_count += 1

    def gender_by_detector(self, name):
        try: 
            us_gender = self.us_detector.guess(name) #Check against US database
            return us_gender
        except Exception as error:
            print "Something wrong at gender_by_detector: {0}".format(error)

    def gender_by_computer(self, name):
        try:
            unicode_name = unicode(name, "utf-8")
            gender = self.gc.resolveGender(unicode_name, 'USA')
            return gender
        except Exception as error:
            print "Something wrong at gender_by_computer: {0}".format(error)

    def gender_by_sex_machine(self, name):
        try:   
            unicode_name = unicode(name, "utf-8")
            gender = self.d.get_gender(unicode_name)
            return gender
        except Exception as error:
            print "Something wrong at gender_by_sex_machine: {0}".format(error)

    def gender_by_guesser(self, name):
        try:
            unicode_name = unicode(name, "utf-8")
            gender = self.gender_guesser.get_gender(unicode_name)
            return gender
        except Exception as error:
            print "Something wrong at gender_by_guesser: {0}".format(error)

    def gender_by_pronoun(self, user):
        is_female = False
        is_male = False
        
        try: 
            all_assignments = user[0]["assignments"]
            all_fp_assignments = all_assignments["fp"]["job"]
            all_hr_assignments = all_assignments["hr"]["job"]
            
            if (type(all_fp_assignments) == list):
                for job in all_fp_assignments:
                    try:
                        try: 
                            feedback = job["feedback"]["comment"]
                            print feedback
                        except:
                            feedback = job["feedback"][0]["comment"]
                            print feedback 
                        is_female = re.search(" her |Her |She | she | her/.", feedback)
                        is_male = re.search(" his |His |He | he | him | him/.", feedback)
                        break 
                    except:
                        continue 
            elif (type(all_fp_assignments == dict)):
                try:
                    feedback = all_fp_assignments["feedback"]["comment"]
                    print feedback
                except:
                    feedback = job["feedback"][0]["comment"]
                    print feedback 
                is_female = re.search(" her |Her |She | she | her/.", feedback)
                is_male = re.search(" his |His |He | he | him | him/.", feedback)
                  
            if not is_female and not is_male:
                if (type(all_hr_assignments) == list):
                    for job in all_hr_assignments: 
                        try:
                            try: 
                                feedback = job["feedback"]["comment"]
                                print feedback
                            except:
                                job["feedback"][0]["comment"]
                                print feedback 
                            is_female = re.search(" her |Her |She | she | her/.", feedback)
                            is_male = re.search(" his |His |He | he | him | him/.", feedback)
                            break 
                        except:
                            continue 
                elif (type(all_hr_assignments) == dict):
                    try: 
                        feedback = all_hr_assignments["feedback"]["comment"]
                        print feedback
                    except:
                        feedback = all_hr_assignments["feedback"][0]["comment"]
                        print feedback
                    is_female = re.search(" her |Her |She | she | her/.", feedback)
                    is_male = re.search(" his |His |He | he | him | him/.", feedback)
                  
        except Exception as error:
            print "Could not find assignments for gender_by_pronoun: {0}".format(error)
            return "unknown" 

        if is_female and not is_male:
            return "female"
        elif is_male and not is_female:
            return "male"
        elif is_male and is_female:
            return "ambiguous"
        else:
            return "unknown" 
                
myObject = UpworkDataFormatter()
myObject.save_all_to_csv()