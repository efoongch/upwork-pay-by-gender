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
        self.present_date = "11/2017" # This is the month in which the data was collected
        self.all_data_file_name = '11_29_2017_upwork_analysis_unitedstates_allskills.csv' # Filename for all data
        self.gender_data_file_name = '11_29_2017_upwork_gender_analysis_unitedstates_allskills.csv' # Filename for gender data
        self.data_log_file_name = 'log_upwork_data_analysis_2017_11_29_unitedstates_allskills.txt'
        self.gender_log_file_name = "log_upwork_identify_gender_2017_11_29_unitedstates_allskills.txt"
        self.github_photo_path = 'https://raw.githubusercontent.com/efoongch/upwork-pay-by-gender/master/resized_profile_photos/resized_images_unitedstates_allskills_2017_11_01/'
        
        # Write a log
        self.log = open(self.data_log_file_name, 'a')
        self.photo_log = open(self.gender_log_file_name, 'a')
        self.log.write("We have started analyzing data!" + "\n")
        self.log.flush()
        self.photo_log.write("We have started identifying gender in photos!" + "\n")
        self.photo_log.flush()

        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
        
        # Get detailed_info from workers in our database
        self.cur.execute("SELECT detailed_info FROM upwork_unitedstates_allskills_2017_11_01;")
        
        # Initialize arrays for Causal Analysis 
        self.user_count = 1
        self.bill_rate_array = []
        self.gender_array = []
        self.all_covariates_array = []
        self.country_id_list = []
        self.job_category_id_list = []
        self.education_id_list = []
        self.error_country_id = 0
        self.error_education_id = 0
        self.error_job_category_id = 0
    
    def save_all_to_csv(self):
        with open(self.all_data_file_name, 'w') as csvfile:
            fieldnames = ['user_count','worker_id', 'first_name', 'bill_rate', 
                          'country', 'country_id', 'degree', 'education', 'education_id', 
                          'work_experience', 'job_category', 'job_category_id']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for user in self.cur:
                try: 
                    user_count = self.user_count
                    worker_id = user[0]["ciphertext"]
                    first_name = user[0]["dev_first_name"].encode('utf-8').strip()
                    bill_rate = user[0]["dev_bill_rate"]
                    country = user[0]["dev_country"]
                    country_id = self.encode_category(country, self.country_id_list)
                    degree = self.show_degree(user)
                    education = self.calculate_education(user)
                    education_id = self.encode_category(education, self.education_id_list)
                    work_experience = self.calculate_work_experience(user)
                    job_category = self.identify_job_category(user)
                    job_category_id = self.encode_category(job_category, self.job_category_id_list)
            
                    writer.writerow({'user_count': user_count, 'worker_id': worker_id, 'first_name': first_name,
                                 'bill_rate': bill_rate, 'country': country, 'country_id': country_id, 'degree': degree, 'education': education, 
                                 'education_id': education_id, 'work_experience': work_experience, 'job_category': job_category, 'job_category_id': job_category_id})
                except:
                    print "Ran into some error at user {0}".format(self.user_count)
                    print json.dumps(user[0], indent=2)
                    
                    writer.writerow({'user_count': user_count, 'worker_id': "error", 'first_name': "error",
                                 'bill_rate': "error", 'country': "error", 'country_id': self.error_country_id, 'degree': "error", 'education': "error", 
                                 'education_id': self.error_education_id, 'work_experience': "error", 'job_category': "error", 'job_category_id': self.error_job_category_id})
                    
                self.user_count += 1
    
    def save_gender_to_csv(self): # Identify gender of users in photos that have already been pushed to GitHub 
        with open(self.gender_data_file_name, 'w') as csvfile:
            fieldnames = ['user_count','worker_id', 'gender']
            gender_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            gender_writer.writeheader()
            
            for user in self.cur:
                try: 
                    user_count = self.user_count
                    worker_id = user[0]["ciphertext"]
                    gender = self.identify_gender()
     
                    gender_writer.writerow({'user_count': user_count, 'worker_id': worker_id, 'gender': gender})
        
                except Exception as err:
                    user_count = self.user_count
                    print "Ran into some error saving gender to csv at {0}".format(self.user_count)
                    self.photo_log.write("Ran into an error saving gender to csv at {0}: {1}".format(self.user_count, err) + "\n")
                    self.photo_log.flush()
                    gender_writer.writerow({'user_count': user_count, 'worker_id': "error", 'gender': "error"})
                    
                self.user_count += 1
        
    def identify_gender(self): # Returns gender as a string
        if (self.user_count%10 == 0): # Set timeout for rate limiting; paid subscription up to 10 per second 
            time.sleep(1)
        
        try: 
            print "Recognizing Face Number " + str(self.user_count)
            raw_face_data = self.recognize_faces()
            
            if(len(raw_face_data) == 0): 
                return "unidentified"
            
            else:
                gender = raw_face_data[0]["faceAttributes"]["gender"]
                return str(gender)
            
        except Exception as err:
            print err
            self.photo_log.write("Ran into an error in identifying gender at {0}: {1}".format(self.user_count, err) + "\n")
            self.photo_log.flush()
            return "error"
        
    def recognize_faces(self):
        # Replace the subscription_key string value with your valid subscription key.
        subscription_key = 'd6ef52f1ff334ace97271267626f11bc' # This is key 1 for the paid subscription 

        # Replace or verify the region.
        uri_base = 'eastus2.api.cognitive.microsoft.com'

        # Request headers.
        headers = {
            'Content-Type': 'application/json',
            'Ocp-Apim-Subscription-Key': subscription_key,
        }

        # Request parameters.
        params = urllib.urlencode({
            'returnFaceId': 'true',
            'returnFaceLandmarks': 'false',
            'returnFaceAttributes': 'age,gender,headPose,smile,facialHair,glasses,emotion,hair,makeup,occlusion,accessories,blur,exposure,noise',
        })

        # The URL of a JPEG image to analyze.
        beg_body = "{'url':"
        end_body = "}"
        path = self.github_photo_path + str(self.user_count) + ".jpg"
        body = beg_body + '"' + path + '"' + end_body
        
        try:
            # Execute the REST API call and get the response.
            conn = httplib.HTTPSConnection('eastus2.api.cognitive.microsoft.com')
            conn.request("POST", "/face/v1.0/detect?%s" % params, body, headers)
            response = conn.getresponse()
            data = response.read()
            parsed = json.loads(data)
            return parsed
            
        except Exception as e:
            print("[Errno {0}] {1}".format(e.errno, e.strerror))
            return "error"
            
        '''
        # 'data' contains the JSON data. The following formats the JSON data for display.
        parsed = json.loads(data)
        print ("Response:")
        print (json.dumps(parsed, sort_keys=True, indent=2))
        conn.close()
        '''
    
    def calculate_work_experience(self, user):
        total_experience = 0
        work_experience_array = []
        
        try: 
            work_experience_list = user[0]["experiences"]["experience"]
            print "were able to find work experiences"
            
        except:
            print "we did not find work experiences"
            return 0
        
        if (type(work_experience_list) is list):
            count = 0
            total_overlap = 0
            for experience in work_experience_list:
                print "This is our first work experience"
                start_date = experience["exp_from"]
                end_date = experience["exp_to"]
                if (end_date == "Present"):
                    end_date = self.present_date
                start_datetime = datetime.datetime.strptime(start_date, "%m/%Y")
                end_datetime = datetime.datetime.strptime(end_date, "%m/%Y")
                work_experience_array.append([start_datetime, end_datetime])
                
                experience_duration = (end_datetime - start_datetime).days
                total_experience += experience_duration
            
            while (count < len(work_experience_array) - 1): # Check for overlap in work experience 
                start_newer = work_experience_array[count][0]
                end_newer = work_experience_array[count][1]
                start_older = work_experience_array[count+1][0]
                end_older = work_experience_array[count+1][1] 
                
                if (end_newer == end_older): # Pattern 1
                    newer_duration = (end_newer - start_newer).days
                    older_duration = (end_older - start_older).days
                    overlap = min(newer_duration, older_duration)
                    total_overlap += overlap 
                    count += 1
                    print "Pattern 1"
                    print overlap
                
                elif(start_older == start_newer): # Pattern 2
                    overlap = (end_older - start_older).days
                    total_overlap += overlap 
                    count += 1
                    print "Pattern 2"
                    print overlap
                
                elif(start_newer < start_older and end_newer > end_older): # Pattern 3
                    overlap = (end_older - start_older).days
                    total_overlap += overlap 
                    count += 1
                    print "Pattern 3"
                    print overlap
                
                elif(start_newer > start_older and end_newer > end_older and end_older > start_newer): # Pattern 4
                    overlap = (end_older - start_newer).days
                    total_overlap += overlap 
                    count += 1
                    print "Pattern 4"
                    print overlap 
                        
                else:
                    print "No overlap"
                    count += 1
                    
            total_experience = (total_experience - total_overlap)/30
            total_experience = total_experience/12 # Convert to years
            
            return total_experience
            
        elif (type(work_experience_list) is dict):
            print "is dict"
            start_date = work_experience_list["exp_from"]
            end_date = work_experience_list["exp_to"]
            if (end_date == "Present"):
                end_date = self.present_date
            start_datetime = datetime.datetime.strptime(start_date, "%m/%Y")
            end_datetime = datetime.datetime.strptime(end_date, "%m/%Y")
            
            
            
            total_experience = ((end_datetime - start_datetime).days)/30
            total_experience = total_experience/12 # Convert to years
            
            return total_experience
        
        else: 
            return 0
    
    def show_degree(self, user):
        try: 
            ed_history = user[0]["education"]["institution"]
            if(ed_history == ""):
                return "No edu listed"
            elif(type(ed_history) == dict):
                return ed_history["ed_degree"]
            elif(type(ed_history) == list):
                degree_list = []
                for ed_experience in user[0]["education"]["institution"]:
                    if (ed_experience["ed_degree"] == ""):
                        degree_list.append("none")
                    elif (ed_experience["ed_degree"] != ""):
                        degree_list.append(ed_experience["ed_degree"])
                return degree_list
            else:
                return "Error: Some other error"
                
        except:
            return "Error: No education"
        
    def calculate_education(self, user):
        try: 
            ed_history = user[0]["education"]["institution"]
            if(ed_history == ""):
                return "None"
            elif(type(ed_history) == dict):
                if (ed_history["ed_degree"] == ""):
                    highest_education = self.check_highest_education("none")
                elif (ed_history["ed_degree"] != ""):
                    highest_education = self.check_highest_education(ed_history["ed_degree"])
                
                # Check for highest level of degree
                if(highest_education == "No edu listed"): # Don't count anything we can't identify
                    return "None"
                elif(highest_education == "Other"): # Don't count anything we can't identify
                    return "None"
                elif(highest_education == "Doctorate"):
                    return "Doctorate"
                elif(highest_education == "Professional"):
                    return "Professional"
                elif(highest_education == "Master"):
                    return "Master"
                elif(highest_education == "Bachelor"):
                    return "Bachelor"
                elif(highest_education == "Associate"):
                    return "Associate"
                elif(highest_education == "High School"):
                    return "High School"
                else:
                    return "None"
      
            elif(type(ed_history) == list):
                ed_list = []
                for ed_experience in user[0]["education"]["institution"]:
                    if (ed_experience["ed_degree"] == ""):
                        highest_education = self.check_highest_education("none")
                        ed_list.append(highest_education)
                    elif (ed_experience["ed_degree"] != ""):
                        highest_education = self.check_highest_education(ed_experience["ed_degree"])
                        ed_list.append(highest_education)
                # Check for highest level of degree
                if("No edu listed" in ed_list): # Don't count anything we can't identify
                    return "None"
                elif("Other" in ed_list): # Don't count anything we can't identify
                    return "None"
                elif ("Doctorate" in ed_list):
                    return "Doctorate"
                elif("Professional" in ed_list):
                    return "Professional"
                elif("Master" in ed_list):
                    return "Master"
                elif("Bachelor" in ed_list):
                    return "Bachelor"
                elif("Associate" in ed_list):
                    return "Associate"
                elif("High School" in ed_list):
                    return "High School"
                else:
                    return "None"
            else:
                return "Error: Some other error"
                
        except:
            return "None"
    
    def check_highest_education(self, degree):
        # Checks for the highest level of education attained by user (DO NOT CHANGE ORDER!) 
        
        # Professional degree
        if (re.search('d\.c|d\.c\.m|d\.d\.s|d\.m\.d|ll\.b|ll\.m|l\.l\.m|l\.l\.b|j\.d\.|m\.d\.|o\.d\.|d\.o\.|pharm\.d', degree.lower()) 
            or re.search('d\.p\.m|d\.p|pod\.d|m\.div|m\.h\.l|b\.d|ordination|d\.v\.m| law', degree.lower())):
            return "Professional"
        
        # Masters
        elif (re.search('Master|Engineer\'s degree|M\.B\.A', degree)):
            return "Master"

        # Bachelors
        elif (re.search('bachelor|b\.a\.|b\.s\.', degree.lower())):
            return "Bachelor"
        

        # Associate's Degree
        elif (re.search('associate|diploma', degree.lower())):
            return "Associate"

        # PhD
        elif (re.search('doctor', degree.lower())):
            return "Doctorate"

        # High School
        elif (re.search('high school', degree.lower())):
            return "High School"

        elif (re.search('none', degree)):
            return "No edu listed"

        else:
            return "Other" # We couldn't recognize the degree 
    
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
myObject.save_gender_to_csv()