import csv
import json
from collections import OrderedDict
from pymongo import MongoClient
import sys, getopt, pprint

# DataBase Connection
connection = MongoClient('localhost', 27017)
print "DB Connected Successfully"

def createDocumentStadium():
    db = connection.Soccer1
    db.Team_Scores.drop()
    getAllTeam = db.Team.distinct('TeamID')
    for teamid in getAllTeam:
        team = db.Team.find_one({'TeamID' : teamid},{'Team' : 1})
        teamdetails = db.Game.find({'$or' :[{'TeamID1' : teamid},{'TeamID2' : teamid}]})
        matchHistoryArray = []
        for data in teamdetails:		
            sname = db.Stadium.find_one({'SID':data['SID']},{'SName':1})
            scity = db.Stadium.find_one({'SID':data['SID']},{'SCity':1})
            team1 = db.Team.find_one({'TeamID':data['TeamID1']},{'Team':1})
            team2 = db.Team.find_one({'TeamID':data['TeamID2']},{'Team':1})
            matchList = {
                'TeamID1': data['TeamID1'],
                'TeamID2': data['TeamID2'],
                'Team1_Score': int(data['Team1_Score']),
                'Team2_Score': int(data['Team2_Score']),
                'MatchDate': data['MatchDate'],
				'SID' : data['SID'],
				'Sname': sname,
				'Scity' : scity,
				'Team1' : team1,
				'Team2' : team2
				
				
            }
            matchHistoryArray.append(matchList)		  

        db.Team_Scores.insert({
            'Team': team,
            'TeamID': teamid,
            'Details': matchHistoryArray
        })
    print "Document Team_Scores Created!!"
if __name__ == '__main__':
    createDocumentStadium()