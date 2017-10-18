import httplib2
import oauth2
import urllib3
import json
import types
import re
import numpy
from gender_detector import GenderDetector 


detector = GenderDetector('us')
user_count = 0
user_ids = []
duplicate = ""
worker_list = []
gender_by_detector = "none"
gender_by_census = "none"
female_detector_count = 0
male_detector_count = 0
unknown_detector_count = 0
female_census_count = 0
male_census_count = 0
unknown_census_count = 0


with open('10_15_2017_java_developers_500_1.json', 'r') as f:
	z = json.load(f)

for i in z:
	user_on_page = 0
	
	for page in i:
		worker_id = i[user_on_page]["id"]
		first_name = i[user_on_page]["name"].split(' ', 1)[0]
		

		if worker_id in user_ids:
			duplicate = " and is a duplicate"

		if worker_id not in user_ids:
			duplicate = ""

		user_count += 1
		user_on_page += 1

		user_ids.append(worker_id)


		# Using gender_detector package
		# gender_by_detector = detector.guess(first_name)

		# Using census data 
		with open("yob1985.txt") as f:
			for line in f:
				if re.search("{0},".format(first_name), line):
					if re.search(",F,", line):
						gender_by_census = "F"
						break
					if re.search(",M,", line):
						gender_by_census = "M"
						break
					if re.search(",U,", line):
						gender_by_census = "U"
						break
				if not re.search("{0},".format(first_name), line):
					gender_by_census = "N"

		worker_list.append([first_name, gender_by_census])


		print "Number " + str(user_count) + " is {0}".format(first_name) + " with id number {0}".format(worker_id) + "who is {0} (census)".format(gender_by_census) + duplicate


# Comparing the effectivness of approaches to identifying gender
'''
for i in worker_list:
	if i[1] == "male":
		male_detector_count += 1
	elif i[1] == "female":
		female_detector_count += 1
	elif i[1] == "unknown":
		unknown_detector_count += 1
'''

for i in worker_list:
	if i[2] == "M":
		male_census_count += 1
	elif i[2] == "F":
		female_census_count += 1
	elif i[2] == "N":
		unknown_census_count += 1


# detector_rate = (male_detector_count + female_detector_count) / float(male_detector_count + female_detector_count + unknown_detector_count) * 100
census_rate = (male_census_count + female_census_count) / float(male_census_count + female_census_count + unknown_census_count) * 100

# print "Using gender_detector package: Male: {0}, Female: {1}, Unknown: {2}. ".format(male_detector_count, female_detector_count, unknown_detector_count) + "This is the rate of identifying gender: {0}%".format(detector_rate)
print "Using census data: Male: {0}, Female: {1}, Unknown: {2}. ".format(male_census_count, female_census_count, unknown_census_count) + "This is the rate of identifying gender: {0}%".format(census_rate)





