import httplib2
import oauth2
import urllib3
import json
import types
import re
import numpy
from gender_detector import GenderDetector 
import psycopg2, psycopg2.extras

class DatabaseAnalyzer: 
    detector = GenderDetector('us')
    
    def __init__(self):
        self.log = open('json_files/log_upwork_data_analysis_2017_10_24_unitedstates_webdev_trial.txt', 'a') # Creating a log
        self.log.write("We have started analyzing data!" + "\n")

        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
        self.user_count = 0
        self.user_ids = []
        self.duplicate = ""
        self.worker_list = []
        self.gender_by_detector = "none"
        self.female_detector_count = 0
        self.male_detector_count = 0
        self.unknown_detector_count = 0

    def identify_gender(self):
        self.cur.execute("SELECT detailed_info FROM upwork_unitedstates_webdev_2017_10_23;")
        for user in self.cur:
            while self.user_count < 1:
                worker_id = user[0]["ciphertext"]
                first_name = user[0]["dev_first_name"]
                print worker_id
                print first_name
                self.user_count += 1
            '''
            worker_id = user["ciphertext"]
            first_name = user["dev_first_name"]
            print "This is worker {0} whose name is {1}".format(worker_id, first_name)
            self.user_count += 1


        print "This is our final user count: {0}".format(self.user_count)
        '''

myObject = DatabaseAnalyzer()
myObject.identify_gender()


'''
    for i in z:
        user_on_page = 0
        
        for page in i:
            worker_id = i[user_on_page]["id"]
            first_name = i[user_on_page]["name"].split(' ', 1)[0]
            
            # Just double checking that there are no duplicates
            if worker_id in user_ids:
                duplicate = " and is a duplicate"

            if worker_id not in user_ids:
                duplicate = ""

            user_count += 1
            user_on_page += 1

            user_ids.append(worker_id)

            # Using gender_detector package
            gender_by_detector = detector.guess(first_name)

            worker_list.append([first_name, gender_by_detector])

            print "Number " + str(user_count) + " is {0}".format(first_name) + " with id number {0}".format(worker_id) + "who is {0} (detector)".format(gender_by_detector) + duplicate


    # Comparing the effectivness of approaches to identifying gender
    for i in worker_list:
        if i[1] == "male":
            male_detector_count += 1
        elif i[1] == "female":
            female_detector_count += 1
        elif i[1] == "unknown":
            unknown_detector_count += 1

    detector_rate = (male_detector_count + female_detector_count) / float(male_detector_count + female_detector_count + unknown_detector_count) * 100


    print "Using gender_detector package: Male: {0}, Female: {1}, Unknown: {2}. ".format(male_detector_count, female_detector_count, unknown_detector_count) + "This is the rate of identifying gender: {0}%".format(detector_rate)
'''




