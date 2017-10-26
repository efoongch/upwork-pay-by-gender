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

    def __init__(self):
        self.log = open('json_files/log_upwork_data_analysis_2017_10_24_unitedstates_webdev_trial.txt', 'a') # Creating a log
        self.log.write("We have started analyzing data!" + "\n")

        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
        self.user_count = 0
        self.worker_list = []
        self.male_list = []
        self.female_list = []
        self.unknown_list = []
        self.problem_list = []
        self.detector = GenderDetector('us')
        self.female_detector_count = 0
        self.male_detector_count = 0
        self.unknown_detector_count = 0
        self.problem_detector_count = 0
        self.detector_rate = 0
        self.male_average_wage = 0
        self.female_average_wage = 0
        self.unknown_average_wage = 0
        self.problem_average_wage = 0
        self.no_detailed_info_count = 0

    def identify_gender(self):
        self.cur.execute("SELECT detailed_info FROM upwork_worldwide_allskills_2017_10_21;")
        for user in self.cur:
            print "Now we're on user number {0}".format(self.user_count)
            try:
                worker_id = user[0]["ciphertext"]
                first_name = user[0]["dev_first_name"]
                bill_rate = float(user[0]["dev_bill_rate"])
                gender_by_detector = "none"

            except:
                print "This one has no detailed info: {0}".format(self.user_count)
                self.no_detailed_info_count += 1

            # Using gender_detector package to detect gender
            try:
                gender_by_detector = self.detector.guess(first_name)
            except:
                gender_by_detector = "problem"

            self.worker_list.append([first_name, gender_by_detector, bill_rate])
            self.user_count += 1

        # Adding bill rate to respective lists
        for i in self.worker_list:
            if i[1] == "male":
                self.male_list.append(i[2])
                self.male_detector_count += 1
            elif i[1] == "female":
                self.female_list.append(i[2])
                self.female_detector_count += 1
            elif i[1] == "unknown":
                self.unknown_list.append(i[2])
                self.unknown_detector_count += 1
            elif i[1] == "problem":
                self.problem_list.append(i[2])
                self.problem_detector_count += 1

        self.detector_rate = (self.male_detector_count + self.female_detector_count) / float(self.user_count) * 100
        self.male_average_wage = numpy.mean(self.male_list)
        self.female_average_wage = numpy.mean(self.female_list)
        self.unknown_average_wage = numpy.mean(self.unknown_list)
        self.problem_average_wage = numpy.mean(self.problem_list)

        print "Using gender_detector package: Male: {0}, Female: {1}, Unknown: {2}, Problem: {3}. ".format(self.male_detector_count, self.female_detector_count, self.unknown_detector_count, self.problem_detector_count) + "This is the rate of identifying gender: {0}%".format(self.detector_rate)
        print "This is our final user count: {0}".format(self.user_count)
        print "Average wages: Male ${0}, Female ${1}, Unknown ${2}, Problem ${3}".format(self.male_average_wage, self.female_average_wage, self.unknown_average_wage, self.problem_average_wage)
        print "And we had these many profiles with empty detailed_info: {0} or {1}%".format(self.no_detailed_info_count, float(self.no_detailed_info_count/self.user_count)*100)
myObject = DatabaseAnalyzer()
myObject.identify_gender()



