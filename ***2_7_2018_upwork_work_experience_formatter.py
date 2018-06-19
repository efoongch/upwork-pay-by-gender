import httplib2
import oauth2
import urllib3
import types
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2, psycopg2.extras
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
        self.present_date = "12/2017" # This is the month in which the data was collected
        self.file_name = './csv_files/work_experience_2017_12_12_upwork_analysis_unitedstates_allskills.csv'
        self.log_file_name = './log_files/log_work_experience_upwork_data_analysis_2017_12_12_unitedstates_allskills.txt'
        
        # Write a log
        self.log = open(self.log_file_name, 'a')
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
    
    def save_work_experience_to_csv(self):
        with open(self.file_name, 'w') as csvfile:
            fieldnames = ['user_count','work_experience']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for user in self.cur:
                try: 
                    work_experience = self.calculate_work_experience(user)
                    writer.writerow({'user_count': self.user_count, 'work_experience': work_experience})
                except KeyboardInterrupt:
                    print "We stoppped"
                    break
                except Exception as error:
                    print "Ran into some error"
                    print error
                    writer.writerow({'user_count': self.user_count, 'work_experience': "error"})
                self.user_count += 1
                print "Finished user {0}".format(self.user_count)
    
    def calculate_work_experience(self, user):
    
        try: 
            work_experience_list = user[0]["experiences"]["experience"]
        except:
            return 0
        
        work_experience_array = []
        all_start_dates = []
        all_end_dates = []
        
        if (type(work_experience_list) is list): # If more than one experience was reported
            for experience in work_experience_list:
                start_date = experience["exp_from"]
                end_date = experience["exp_to"]
                if (end_date == "Present"):
                    end_date = self.present_date
                
                
                start_datetime = datetime.datetime.strptime(start_date, "%m/%Y")
                end_datetime = datetime.datetime.strptime(end_date, "%m/%Y")

                if (start_datetime > end_datetime): # If the dates are somehow flipped
                    return "error"
            
                else: 
                    all_start_dates.append(start_datetime)
                    all_end_dates.append(end_datetime)
                    work_experience_array.append([start_date, end_date])
        
            # Find the newest end_date and oldest start date
            latest_date_formatted = max(all_end_dates)
            oldest_date_formatted = min(all_start_dates)
            latest_date_unformatted = latest_date_formatted.strftime("%m/%Y")
            oldest_date_unformatted = oldest_date_formatted.strftime("%m/%Y")

            # Find the number of months between those dates and create an array of that length
            months = (latest_date_formatted - oldest_date_formatted).days/30

            # Assign a month and a year for each of the elements in that array
            timeline = []

            year = int(oldest_date_unformatted[3:7])
            month = int(oldest_date_unformatted[0:2])

            timeline.append([month, year, True]) #Set values for the oldest start date

            for i in range(months):
                # Add months and years
                if month >= 12: 
                    month = 1
                    year += 1
                else:
                    month += 1

                timeline.append([month, year, False])

            # Loop through the array and fill the element if it falls between a start and end date
            for experience in work_experience_array:
                already_hit = False

                start_month = int(experience[0][0:2])
                start_year = int(experience[0][3:])
                end_month = int(experience[1][0:2])
                end_year = int(experience[1][3:])

                for place in timeline:
                    month = place[0]
                    year = place[1]

                    # Add the work experience to the timeline
                    if (start_month == month and start_year == year):
                        place[2] = True
                        already_hit = True

                    elif (end_month == month and end_year == year):
                        place[2] = True
                        break

                    elif (already_hit):
                        place[2] = True

            print "This is the final timeline filled: {0}".format(timeline)

            # Count all of the filled elements in the array and return 
            total_months_worked = 0

            for place in timeline:
                if place[2] == True:
                    total_months_worked += 1

            print "This is total months worked: {0}".format(total_months_worked)
            return total_months_worked/12 # Return everything in years
   
        elif (type(work_experience_list) is dict): # If only one experience reported, just calculate duration
            start_date = work_experience_list["exp_from"]
            end_date = work_experience_list["exp_to"]
            if (end_date == "Present"):
                end_date = self.present_date
            start_datetime = datetime.datetime.strptime(start_date, "%m/%Y")
            end_datetime = datetime.datetime.strptime(end_date, "%m/%Y")
            
            if (start_datetime >= end_datetime):
                print "the dates were flipped or too short time"
                return "error"
            
            total_experience = ((end_datetime - start_datetime).days)/30
            total_experience = total_experience/12 # Convert to years
          
            return total_experience
        

myObject = UpworkDataFormatter()
myObject.save_work_experience_to_csv()