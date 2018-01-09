import httplib2
import oauth2
import urllib3
import json
import types
import re
import numpy

worker_list = []
unidentified_list = []
none_list = []
female_list = []
male_list = []
unidentified_count = 0
none_count = 0
female_count = 0
male_count = 0
gender = "F"
total_hourly_wages = 0
number_of_jobs = 0



# Reads jsonData (something wrong with this, should be able to read this as a dictionary but it currently doesn't) Next, look for best ways to analyze json data?
with open('10_9_2017_java_developers_3.json', 'r') as f:
	z = json.load(f)
	

for i in z:
	# Print worker ID
	worker_id = i["ciphertext"]
	print worker_id

	# Check if the first name is in the list of names
	first_name = i["dev_first_name"]
	
	with open("yob1985.txt") as f:
		for line in f:
			if re.search("{0},".format(first_name), line):
				if re.search(",F,", line):
					gender = "F"
					break
				if re.search(",M,", line):
					gender = "M"
					break
				if re.search(",U,", line):
					gender = "U"
					break
			if not re.search("{0},".format(first_name), line):
				gender = "N"

	# Check the workers expected wage
	expected_wage = float(i["dev_bill_rate"])
	print "This is their expected wage"
	print expected_wage

	worker_list.append([gender, expected_wage, first_name])

print "This is the worker_list"
print worker_list

for i in worker_list:
	if i[0] == 'U':
		unidentified_list.append(i[1])
		unidentified_count += 1
	elif i[0] == 'N':
		none_list.append(i[1])
		none_count += 1
	elif i[0] == 'F':
		female_list.append(i[1])
		female_count += 1
	elif i[0] == 'M':
		male_list.append(i[1])
		male_count += 1

print "This is the mean for {0} unidentified".format(unidentified_count)
print numpy.mean(unidentified_list)

print "This is the mean for {0} not in dataset".format(none_count)
print numpy.mean(none_list)

print "This is the mean for {0} female".format(female_count)
print numpy.mean(female_list)

print "This is the mean for {0} male".format(male_count)
print numpy.mean(male_list)

'''
	# Calculate how much they have made in hourly assignments total and on average
	print "--------------------THIS IS ONE WORKER-------------------"
	hourly_jobs = i["assignments"]["hr"]["job"]
	print json.dumps(hourly_jobs)
	print "This is the type" + str(type(hourly_jobs))

	if isinstance(hourly_jobs, list):
		for job in hourly_jobs[0]:
			rate = job["as_rate"]
			rate_float = float(rate[1:len(rate)])
			total_hours = float(job["as_total_hours_precise"])
			total_hourly_wages += rate_float * total_hours
			number_of_jobs += 1
			print "rate is " + str(rate_float) + " and total hours is " + str(total_hours) + " and total wages is " + str(total_hourly_wages)
			print "Current nmber of jobs for this worker is " + str(number_of_jobs)
		for job in hourly_jobs[1]:
			number_of_jobs += 1
			print "Current nmber of jobs for this worker is " + str(number_of_jobs)

	elif isinstance(hourly_jobs, dict):
		rate = hourly_jobs["as_rate"]
		rate_float = float(rate[1:len(rate)])
		total_hours = float(hourly_jobs["as_total_hours_precise"])
		total_hourly_wages += rate_float * total_hours
		number_of_jobs += 1
		print "rate is " + str(rate_float) + " and total hours is " + str(total_hours) + " and total wages is " + str(total_hourly_wages)
	
	
	worker_list.append([gender, total_hourly_wages]) 
	

print worker_list

'''