#references:
#http://stackoverflow.com/questions/372885/how-do-i-connect-to-a-mysql-database-in-python
#http://stackoverflow.com/questions/10839475/convert-sql-into-json-in-python
#https://docs.python.org/3.3/library/json.html


import mysql.connector 
import json
import collections
from pymongo import MongoClient


#Connection to mysql
cnx = mysql.connector.connect(user='', password='', host='', database='')

#Connection to MongoDB client using PyMongo
client = MongoClient()
db = 
collection = db.Stadium

try:
	#Execute a mysql query to fetch all the stadium and host city names
	cursor = cnx.cursor()
	cursor.execute("select m.stadium, m.host_city from match_results as m group by m.stadium;")
	result = cursor.fetchall()
	objects_list = []
	#for each of the stadium, execute a mysql query to fetch the match[] attributes
	for row in result:
		cursor.execute("select m2.team1, m2.team2,m2.team1_score, m2.team2_score, m2.dates from match_results as m2 where m2.stadium ='%s'"%row[0])
		output = cursor.fetchall()
		
					
		for data in output:
		
		#create a collection and store the match [] attributes in a dictionary format
			d = collections.OrderedDict()
			d['Team1'] = data[0]
			d['Team2'] = data[1]
			d['Team1Score'] = int(data[2])
			d['Team2Score'] = int(data[3])
			d['Dates'] = data[4]
			
			objects_list.append(d)
	
		s = {"Stadium" : row[0], "City" : row[1], "Match" : objects_list }
		#dump the result in the form of json to a file
		j = json.dumps(s, sort_keys = False, separators = (',',':'), indent = 4)
		#insert the record into mongoDb stadium collection
		db.Stadium.insert(s)
		print j
		#write the json output to a file
		objects_file = 'stadium.json'
		f = open(objects_file,'a')
		print >> f, j
		objects_list = []
			
				
#close the mysql db connection   
finally:
    cnx.close()
	




