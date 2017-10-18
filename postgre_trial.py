# Trying out connecting to postgre

import psycopg2

try:
	conn = psycopg2.connect("dbname='eurekadb' user='eureka' host='localhost' password='yasteam'")
except:
	print "I am unable to connect to the database"