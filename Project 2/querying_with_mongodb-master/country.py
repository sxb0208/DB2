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
collection = db.Country

try:
	# execute a sql query to get the country information
	cursor = cnx.cursor()
	cursor.execute("select c.country_name , c.capital, c.population, c.manager from country as c;")
	country_Output = cursor.fetchall()
	#create a list to store the players and the world cup won history information
	objects_list = []
	won_list = []

	#execute a sql query to fetch the player information for each country
	for row in country_Output:
		cursor.execute("select p.lname,p.fname, p.height, p.dob, p.is_captain, p.position, p.player_id from players as p where p.country = '%s'"%row[0])
		players_Output = cursor.fetchall()
		for playersData in players_Output:
		
			#create a collection in dictionary format and store the information
			d = collections.OrderedDict()
			d['LName'] = playersData[0]
			d['FName'] = playersData[1]
			d['Height'] = float(playersData[2])
			d['DOB'] = playersData[3]
			d['IsCaptain'] = playersData[4]
			d['Position'] = playersData[5]
			d['playerID'] = playersData[6]
			d['no_yellow_cards'] = "null"
			d['no_red_cards'] = "null"
			d['no_Goals'] = "null"
			d['no_Assists'] = "null"
			
			#execute a sql query to fetch the data from player_cards infromation
			cursor.execute("select pc.player_id,pc.no_of_yellow_cards, pc.no_of_red_cards from player_card as pc where pc.player_id='%s'"%playersData[6])
			cards_output = cursor.fetchall()
			
			for cardsdata in cards_output:
				d['no_yellow_cards'] = int(cardsdata[1])
				d['no_red_cards'] = int(cardsdata[2])
			
			#execute a query to fecth the data from player_assists_goals information
			cursor.execute("select pa.goals, pa.assists, pa.player_id from player_assists_goals as pa where pa.player_id='%s'"%playersData[6])
			assist = cursor.fetchall()
			
			for assistsdata in assist:
				d['no_Goals'] = int(assistsdata[0])
				d['no_Assists'] = int(assistsdata[1])
			
			#append the data into the list
			objects_list.append(d)
	
			#execute a query to fetch the worldcup_won_history information
			cursor.execute("select w.year, w.host from world_cup_history as w where w.winner = '%s'"%row[0])
			won = cursor.fetchall()

			for eachwin in won:
				win = collections.OrderedDict()
				win['Year'] = eachwin[0]
				win['Host'] = eachwin[1]
			
				won_list.append(win)
		
		pop = float(row[2])
		s = {"Cname" : row[0], "Capital" : row[1], "Population" : pop, "Manager" : row[3], "Players" : objects_list , "History" : won}
		#dump the result in the form of json to a file
		j = json.dumps(s, sort_keys = False, separators = (',',':'), indent = 4)
		#insert the record into mongoDb statium collection
		db.Country.insert(s)
		#write the json output to a file
		print j
		objects_file = 'country.json'
		f = open(objects_file,'a')
		print >> f, j
		#reset the collections
		objects_list = []
		won_list = []

		
#close the mysql db connection   
finally:
	cnx.close()








