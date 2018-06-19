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
        self.edu_data_file_name = './csv_files/edu6_whole_2017_12_12_upwork_analysis_unitedstates_allskills.csv' # Filename for all data
        self.edu_data_log_file_name = './log_files/edu6_whole_log_upwork_data_analysis_2017_12_12_unitedstates_allskills.txt'
        
        # Write a log
        self.log = open(self.edu_data_log_file_name, 'a')
        self.log.write("We have started analyzing data!" + "\n")
        self.log.flush()

        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
        
        # Get detailed_info from workers in our database
        self.cur.execute("SELECT detailed_info FROM upwork_unitedstates_allskills_2017_12_12;")
        
        # Initialize values 
        self.user_count = 1
        self.education_levels = ["Bachelor or Higher", "Some College or Associate",
                                 "High School", "Less Than High School", "Other", "None"]
        
    
    def show_education(self):

        with open(self.edu_data_file_name, 'w') as csvfile:
            fieldnames = ['user_count','education','education_id', 'ed_list']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for user in self.cur:
                try: 
                    print "This is the user count: {0}".format(self.user_count)
                    user_count = self.user_count
                    education_list = self.return_edu_experience(user)
                    highest_education = self.find_highest_education(education_list)
                    education_id = str(self.education_levels.index(highest_education))
                    degree = ""
                    school = ""
                    comment = ""
                    
                    print "----This is the highest education list {0}----".format(education_list)
                    print "------This is highest education: {0}, {1}-----".format(highest_education, type(highest_education))
                    print "------This is education id: {0}, {1}------".format(education_id, type(education_id))
                    
                    ed_history = user[0]["education"]["institution"]
                    ed_list = []
                    
                    if (highest_education == "Other"): # Show the actual degrees and schools of users we can't identify 
                        if (type(ed_history) == dict):
                            degree = str(ed_history["ed_degree"].encode('utf-8'))
                            school = str(ed_history["ed_school"].encode('utf-8'))
                            comment = str(ed_history["ed_comment"].encode('utf-8'))
                            ed_list.append([degree, school, comment])
                        elif (type(ed_history) == list):
                            for item in ed_history:
                                degree = str(item["ed_degree"].encode('utf-8'))
                                school = str(item["ed_school"].encode('utf-8'))
                                comment = str(item["ed_comment"].encode('utf-8'))
                                ed_list.append([degree, school, comment])

                    writer.writerow({'user_count': self.user_count, 'education': highest_education, 
                                 'education_id': education_id, 'ed_list': ed_list})
                    
                    print "Finished writing data for {0}".format(self.user_count)

                except KeyboardInterrupt: 
                    print "We stopped!"
                    break
                
                except:
                    print "Ran into some error at user {0}".format(self.user_count)
                    
                    writer.writerow({'user_count': self.user_count, 'education': "error", 
                                 'education_id': "error", 'ed_list': "error"})
                
                self.user_count += 1
                
                    
    def find_highest_education(self, edu_list):
        for level in self.education_levels:
            if level in edu_list:
                return level
        return "None"
                
    def return_edu_experience(self, user):
        all_education = []
        try: 
            ed_history = user[0]["education"]["institution"]
            
            if (ed_history == ""):
                print "No ed history"
                return all_education
            
            if (type(ed_history) == dict):
                print "Found a single ed history"
                degree = str(ed_history["ed_degree"].encode('utf-8'))
                school = str(ed_history["ed_school"].encode('utf-8'))
                comment = str(ed_history["ed_comment"].encode('utf-8'))
                
                print "Degree: {0}".format(degree)
                print "School: {0}".format(school)
                print "Comment: {0}".format(comment)
                
                education = self.check_education(degree, school, comment)
                
                has_bachelor_or_higher = re.search("Professional|Doctorate|Master|Bachelor", education)
                has_some_college = re.search("Associate|Certificate|Some College", education)
                has_high_school = re.match("High School", education)
                has_less_than_high_school = re.match("Less Than High School", education)
                
                if (has_high_school): # Check for high school first 
                    all_education.append("High School")
                elif (has_less_than_high_school):
                    all_education.append("Less Than High School")
                elif (has_bachelor_or_higher):
                    all_education.append("Bachelor or Higher")
                elif (has_some_college):
                    all_education.append("Some College or Associate")
                else:
                    all_education.append("Other")
                
                return all_education

            elif (type(ed_history) == list):
                for item in ed_history:
                    degree = str(item["ed_degree"].encode('utf-8'))
                    school = str(item["ed_school"].encode('utf-8'))
                    comment = str(item["ed_comment"].encode('utf-8'))
                    
                    print "Degree: {0}".format(degree)
                    print "School: {0}".format(school)
                    print "Comment: {0}".format(comment)
                
                    education = self.check_education(degree, school, comment)
                    has_bachelor_or_higher = re.search("Professional|Doctorate|Master|Bachelor", education)
                    has_some_college = re.search("Associate|Certificate|Some College", education)
                    has_less_than_high_school = re.search("Less Than High School", education)
                    has_high_school = re.search("High School", education)
                    
                    if (has_high_school): # Check for high school first 
                        all_education.append("High School")
                    elif (has_less_than_high_school):
                        all_education.append("Less Than High School")
                    elif (has_bachelor_or_higher):
                        all_education.append("Bachelor or Higher")
                    elif (has_some_college):
                        all_education.append("Some College or Associate")
                    else:
                        all_education.append("Other")
                        
                return all_education
        
        except Exception as error:
            print "We ran into an error at return edu experience"
            print error
            return all_education
        
    def check_education(self, degree, school, comment): # Do a quick check that the user has a high school degree
        try:
            hs_degree_mentioned = False
            diploma_degree_mentioned = False
            hs_word_mentioned = False
            graduated_mentioned = False
            self_taught_mentioned = False
            middle_school_mentioned = False
            home_school_mentioned = False
           
            if degree != "":
                hs_degree_mentioned = re.search("high school", degree.lower())
                diploma_degree_mentioned = re.search("diploma", degree.lower())
                
            if school != "":
                hs_word_mentioned = re.search("high school| high", school.lower())
                middle_school_mentioned = re.search("middle school", school.lower())
                home_school_mentioned = re.search("homeschool|home school", school.lower()) # If homeschooled and no degree mentioned 
                self_taught_mentioned = re.search("self taught|self-taught", school.lower()) 
            
            if comment != "":
                graduated_mentioned = re.search("graduated", comment.lower())

            if (hs_degree_mentioned or (hs_word_mentioned and (graduated_mentioned or diploma_degree_mentioned))):
                return "High School"
            elif ((hs_word_mentioned and degree == "") or (home_school_mentioned and (graduated_mentioned or diploma_degree_mentioned)) or middle_school_mentioned or self_taught_mentioned):
                return "Less Than High School"
            elif (self.check_school_by_name("high_school", school)): # Check if the school name is in the database
                print "We found the high school!"
                if (hs_degree_mentioned or diploma_degree_mentioned or graduated_mentioned):
                    return "High School"
                elif (degree == ""):
                    return "Less Than High School"
            else:
                print "We did not find the high school; We should be checking the specific degree now!! "
                return self.check_specific_degree(degree, school, comment)
        except Exception as error:
            print "Something went wrong"
            print error
            return "Error"
        
    def check_specific_degree(self, degree, school, comment): # Do a quick check that the user has a bachelor or associate degree

        try:
            
            # Professional degree
            if (re.search('d\.c|d\.c\.m|d\.d\.s|d\.m\.d|ll\.b|ll\.m|l\.l\.m|l\.l\.b|j\.d\.|m\.d\.|o\.d\.|d\.o\.|pharm\.d', degree.lower()) 
                or re.search('d\.p\.m|d\.p|pod\.d|m\.div|m\.h\.l|b\.d|ordination|d\.v\.m| law', degree.lower())
                or re.search('DC|DCM|DDS|DMD|LLB|LLM|LLB|JD|MD|OD|DO|PharmD|Ed\.D|EdD|CFA', degree)
                or re.search('DPM|DP|PodD|MDiv|MHL|BD|DVM', degree)):
                return "Professional"

            # PhD
            elif (re.search('Ph\.D|PhD', degree)
                 or re.search('doctor|licentiate', degree.lower())): # Doctorate placed here in case MDs are also called doctors 
                return "Doctorate"

            # Masters
            elif (re.search('M\.B\.A|MBA|M\.A|M\.S|M\.Sc|M\.E|MA|MS|MSc|ME|M\.A\.Ed|MAEd|MST|M\.S\.T|MEd|M\.Ed|MPA|M\.P\.A', degree)
                 or re.search('master|engineer\'s degree', degree.lower())
                 or re.search('MFA|M\.F\.A|MSC|MCS', degree)):
                return "Master"

            # Bachelors
            elif (re.search('B\.A|B\.S|B\.Sc|BA|BS|BSc|BFA|AB|A\.B|BPharm|B\.Pharm', degree)
                 or re.search('bachelor|bpharm|bachlor|batchelor', degree.lower())):
                return "Bachelor"

            # Associate's Degree
            elif (re.search('AA|AS|A\.A|A\.S|A\.A\.S|AAS', degree)
                 or re.search('associate|foundation', degree.lower())):
                return "Associate"

            # Certificate
            elif (re.search('certificate', degree.lower())):
                return "Certificate"
            
            # If there is no degree or an ambiguous degree, then check if they are enrolled in a college or university
            elif (self.check_school_by_name("college", school)):
                return "Some College"
            
            else:
                print "Ambiguous" 
                return "Ambiguous"
                
            
        except Exception as error:
            print "We ran into an error specifying the degree"
            print error
            return "Error"

            
    def check_school_by_name(self, dataset, name): # Checks if school name is in a dataset of schools 
        name = name.lower()
        name = self.remove_punctuation(name)
        private_schools = 'high_schools_in_us.csv'
        public_schools = 'high_schools_in_us.csv'
        colleges = 'colleges_in_us.csv'
        
        if (dataset == "high_school"): # Check for "high school" and against list of private and public high schools
            if re.search('highschool| high', name):
                return True
            else:
                return False
            
            ''' Temporarily commenting this out to see if the check is any faster 
            with open(private_schools, 'r') as f:
                private_school_data = csv.DictReader(f)
                for row in private_school_data:
                    school = row["pinst"].lower()
                    school = self.remove_punctuation(school)
                    if (name == school):
                        return True
                   
                    
            with open(public_schools, 'r') as f: # Haven't been able to find a suitable dataset 
                public_school_data = csv.DictReader(f)
                for row in public_school_data:
                    school = row["pinst"].lower()
                    school = self.remove_punctuation(school)
                    if (name == school):
                        return True
            
            return False # Return false if we can't find the school name in any of the lists
            '''
       
                
        elif (dataset == "college"): # Check for keywords and against list of colleges
            if (re.search('college|university|institute', name)):
                return True 
            else:
                return False
            
            '''
            with open(colleges, 'r') as f:
                college_data = csv.DictReader(f)
                for row in college_data:
                    school = row["INSTNM"].lower()
                    school = self.remove_punctuation(school)
                    if (name == school):
                        print "Found a matching college!"
                        print name
                        return True
            
            return False # Return false if we can't find the school name in any of the lists
            '''
        
        else:
            return False # Return false if all else fails 
    
    def remove_punctuation(self, word): # Removes all punctuation and blank spaces from a string
        modified_word = re.sub(r'[^\w\s]','',word)
        modified_word = re.sub(' ', '', modified_word)
        return modified_word
                    
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
                    
                    
myObject = UpworkDataFormatter()
myObject.show_education()