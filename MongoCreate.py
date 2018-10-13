import csv
import json
from collections import OrderedDict
from pymongo import MongoClient
import sys, getopt, pprint

# DataBase Connection
connection = MongoClient('localhost', 27017)
print "DB Connected Successfully"

# Create JSON file from given CSV input
def createJson():
    files = ['Team', 'Stadium', 'Players', 'Game', 'Starting_Lineups', 'Goals']
    directory = "C:\\Users\\subramanya\\Desktop\\DB2\\Project 2\\MongoDB-master\\SourceCode\\"
    

    for file in files:
        inputfile = directory + file + ".csv"
        outfile = directory + file + ".json"
        csvfile = open(inputfile, 'r')
        jsonfile = open(outfile, 'w')
        fieldnames = ''
		
        if file == 'Team':
            fieldnames = ("TeamID", "Team", "Continent", "League", "Population")			
        if file == 'Stadium':
            fieldnames = ("SID", "SName", "SCity", "SCapacity")
        if file == 'Players':
            fieldnames = ("Team","TeamID","PNo","Position","PName","Birth_Date","Shirt_Name","Club","Height","Weight")	
        if file == 'Game':
            fieldnames = ("GameID","MatchType","MatchDate","SID","TeamID1","TeamID2","Team1_Score","Team2_Score")
        if file == 'Starting_Lineups':
            fieldnames = ("GameID","TeamID","PNo")
        if file == 'Goals':
            fieldnames = ("GameID","TeamID","PNo","Time","Penalty")
        
        reader = csv.DictReader(csvfile, fieldnames)
        for row in reader:
            json.dump(row, jsonfile, sort_keys=True)
            jsonfile.write("\n")
            			
    print "JSON Files Created Successfully"

# Insert JSON file into MongoDB
def insertIntoDB():
    files=['Team', 'Stadium', 'Players', 'Game', 'Starting_Lineups', 'Goals']
    directory = "C:\\Users\\subramanya\\Desktop\\DB2\\Project 2\\MongoDB-master\\SourceCode\\"

    for file in files:
        inputfile = directory + file + ".csv"
        print inputfile
        csvfile = open(inputfile, 'r')
        fieldnames = ''

        if file == 'Team':
            db = connection.Soccer1.Team
            db.drop()
            fieldnames = ("TeamID", "Team", "Continent", "League", "Population")
            for row in csvfile:
                db.insert({fieldnames[0]: row.split(',')[0],
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2],
                           fieldnames[3]: row.split(',')[3],
                           fieldnames[4]: row.split(',')[4]
                           })
        if file == 'Game':
            db = connection.Soccer1.Game
            db.drop()
            fieldnames = ("GameID","MatchType","MatchDate","SID","TeamID1","TeamID2","Team1_Score","Team2_Score")
            for row in csvfile:
                db.insert({fieldnames[0]: row.split(',')[0],
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2],
                           fieldnames[3]: row.split(',')[3],
                           fieldnames[4]: row.split(',')[4],
                           fieldnames[5]: row.split(',')[5],
                           fieldnames[6]: float(row.split(',')[6]),
                           fieldnames[7]: float(row.split(',')[7])
                           })
        if file == 'Stadium':
            db = connection.Soccer1.Stadium
            db.drop()
            fieldnames = ("SID", "SName", "SCity", "SCapacity")
            for row in csvfile:
                db.insert({fieldnames[0]: row.split(',')[0],
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2],
                           fieldnames[3]: float(row.split(',')[3]),

                           })
        if file == 'Starting_Lineups':
            db = connection.Soccer1.Starting_Lineups
            db.drop()
            fieldnames = ("GameID","TeamID","PNo")
            for row in csvfile:
                db.insert({fieldnames[0]: row.split(',')[0],
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2]

                           })
        if file == 'Players':
            db = connection.Soccer1.Players
            db.drop()
            fieldnames = ("Team","TeamID","PNo","Position","PName","Birth_Date","Shirt_Name","Club","Height","Weight")
            for row in csvfile:
                db.insert({fieldnames[0]: row.split(',')[0],
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2],
                           fieldnames[3]: row.split(',')[3],
                           fieldnames[4]: row.split(',')[4],
                           fieldnames[5]: row.split(',')[5],
                           fieldnames[6]: row.split(',')[6],
                           fieldnames[7]: row.split(',')[7],
                           fieldnames[8]: float(row.split(',')[8]),
                           fieldnames[9]: float(row.split(',')[9])
                           })
        if file == 'Goals':
            db = connection.Soccer1.Goals
            db.drop()
            fieldnames = ("GameID","TeamID","PNo","Time","Penalty")
            for row in csvfile:
                db.insert({fieldnames[0]: row.split(',')[0],
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2],
						   fieldnames[3]: row.split(',')[3]
                           })
    print "Data Inserted Into MongoDB Successfully"


#Call the created functions
if __name__ == '__main__':
    createJson()
    insertIntoDB()
   



	