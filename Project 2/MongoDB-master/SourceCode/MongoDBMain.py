######################################################
#   Name : Sagar Chhadia
#   ID   : 1001213987
######################################################

import csv
import json
from pymongo import MongoClient
import sys, getopt, pprint

# DataBase Connection
connection = MongoClient('localhost', 27017)
print "DB Connected Successfully"

# Create JSON file from given CSV input
def createJson():
    files = ['Country', 'Match_results', 'Player_Assists_Goals', 'Player_Cards', 'Players', 'Worldcup_History']
    directory = "C:\Users\subramanya\Desktop\DB2\MongoDB-master\\"

    for file in files:
        inputfile = directory + file + ".csv"
        outfile = directory + file + ".json"
        csvfile = open(inputfile, 'r')
        jsonfile = open(outfile, 'w')
        fieldnames = ''

        if file == 'Country':
            fieldnames = ("Country_Name", "Population", "No_of_Worldcup_won", "Manager", "Capital")
        if file == 'Match_results':
            fieldnames = (
            "Match_id", "Date", "Start_time", "Team1", "Team2", "Team1_score", "Team2_score", "Stadium", "Host_city")
        if file == 'Player_Assists_Goals':
            fieldnames = ("Player_id", "No_of_Matches", "Goals", "Assists", "Minutes_Played")
        if file == 'Player_Cards':
            fieldnames = ("Player_id", "No_of_Yellow_cards", "No_of_Red_cards")
        if file == 'Players.csv':
            fieldnames = (
            "Player_id", "Name", "Fname", "Lname", "DOB", "Country", "Height", "Club", "Position", "Caps_for_country",
            "Is_captain")
        if file == 'Worldcup_History':
            fieldnames = ("Year", "Host", "Winner")

        reader = csv.DictReader(csvfile, fieldnames)
        for row in reader:
            json.dump(row, jsonfile)
            jsonfile.write('\n')
    print "Json Files Created Successfully"


# Insert JSON file into MongoDB
def insertIntoDB():
    files=['Country', 'Match_results', 'Player_Assists_Goals', 'Player_Cards', 'Players', 'Worldcup_History']
    directory = "C:\Users\subramanya\Desktop\DB2\MongoDB-master\\"

    for file in files:
        inputfile = directory + file + ".csv"
        print inputfile
        csvfile = open(inputfile, 'r')
        fieldnames = ''

        if file == 'Country':
            db = connection.Soccer.Country_Data
            db.drop()
            fieldnames = ("Country_Name", "Population", "No_of_Worldcup_won", "Manager", "Capital")
            for row in csvfile:
                db.insert({fieldnames[0]: row.split(',')[0],
                           fieldnames[1]: float(row.split(',')[1]),
                           fieldnames[2]: float(row.split(',')[2]),
                           fieldnames[3]: row.split(',')[3],
                           fieldnames[4]: row.split(',')[4],
                           })
        if file == 'Match_results':
            db = connection.Soccer.Match_results_Data
            db.drop()
            fieldnames = ("Match_id", "Date", "Start_time", "Team1", "Team2", "Team1_score", "Team2_score", "Stadium", "Host_city")
            for row in csvfile:
                db.insert({fieldnames[0]: float(row.split(',')[0]),
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2],
                           fieldnames[3]: row.split(',')[3],
                           fieldnames[4]: row.split(',')[4],
                           fieldnames[5]: float(row.split(',')[5]),
                           fieldnames[6]: float(row.split(',')[6]),
                           fieldnames[7]: row.split(',')[7],
                           fieldnames[8]: row.split(',')[8]
                           })
        if file == 'Player_Assists_Goals':
            db = connection.Soccer.Player_Assists_Goals_Data
            db.drop()
            fieldnames = ("Player_id", "No_of_Matches", "Goals", "Assists", "Minutes_Played")
            for row in csvfile:
                db.insert({fieldnames[0]: float(row.split(',')[0]),
                           fieldnames[1]: float(row.split(',')[1]),
                           fieldnames[2]: float(row.split(',')[2]),
                           fieldnames[3]: float(row.split(',')[3]),
                           fieldnames[4]: float(row.split(',')[4])

                           })
        if file == 'Player_Cards':
            db = connection.Soccer.Player_Cards_Data
            db.drop()
            fieldnames = ("Player_id", "No_of_Yellow_cards", "No_of_Red_cards")
            for row in csvfile:
                db.insert({fieldnames[0]: float(row.split(',')[0]),
                           fieldnames[1]: float(row.split(',')[1]),
                           fieldnames[2]: float(row.split(',')[2])

                           })
        if file == 'Players':
            db = connection.Soccer.Players_Data
            db.drop()
            fieldnames = ("Player_id", "Name", "Fname", "Lname", "DOB", "Country", "Height", "Club", "Position", "Caps_for_country", "Is_captain")
            for row in csvfile:
                db.insert({fieldnames[0]: float(row.split(',')[0]),
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2],
                           fieldnames[3]: row.split(',')[3],
                           fieldnames[4]: row.split(',')[4],
                           fieldnames[5]: row.split(',')[5],
                           fieldnames[6]: float(row.split(',')[6]),
                           fieldnames[7]: row.split(',')[7],
                           fieldnames[8]: row.split(',')[8],
                           fieldnames[9]: float(row.split(',')[9]),
                           fieldnames[10]: row.split(',')[10]
                           })
        if file == 'Worldcup_History':
            db = connection.Soccer.Worldcup_History_Data
            db.drop()
            fieldnames = ("Year", "Host", "Winner")
            for row in csvfile:
                db.insert({fieldnames[0]: row.split(',')[0],
                           fieldnames[1]: row.split(',')[1],
                           fieldnames[2]: row.split(',')[2]
                           })
    print "Data Inserted Into MongoDB Successfully"


# Create Country Document
def createDocumentCountry():
    db = connection.Soccer
    db.Country.drop()
    getAllCountries = db.Country_Data.find()

    for country in getAllCountries:
        # Get Players who are playing match for the country
        getPlayers = db.Players_Data.find({'Country': country['Country_Name']})
        playerArray = []
        for player in getPlayers:
            # Get card details for the player
            playerCards = db.Player_Cards_Data.find_one({'Player_id': player['Player_id']})
            noOfYellowCards = 0.0
            noOfRedCards = 0.0
            if (playerCards):
                noOfYellowCards = playerCards['No_of_Yellow_cards']
                noOfRedCards = playerCards['No_of_Red_cards']

            # Get Goal Assists by the player
            playerAssistsGoals = db.Player_Assists_Goals_Data.find_one({'Player_id': player['Player_id']})
            noOfGoals = 0.0
            noOfAssists = 0.0
            if (playerAssistsGoals):
                noOfGoals = playerAssistsGoals['Goals']
                noOfAssists = playerAssistsGoals['Assists']

            playersList = {
                'Player_id': int(player['Player_id']),
                'Fname': player['Fname'],
                'Lname': player['Lname'],
                'Height': float(player['Height']),
                'DOB': player['DOB'],
                'Is_captain': player['Is_captain'],
                'Position': player['Position'],
                'No_of_Yellow_cards': int(noOfYellowCards),
                'No_of_Red_cards': int(noOfRedCards),
                'Goals': int(noOfGoals),
                'Assists': int(noOfAssists)
            }
            playerArray.append(playersList)

        # Get World Cup History for the country
        worldcupHistoryArray = []
        if (country['No_of_Worldcup_won'] == 0):
            worldcupHistoryArray = []
        else:
            worldCupsWon = db.Worldcup_History_Data.find({'Winner': country['Country_Name']})
            for worldCup in worldCupsWon:
                worldCupList = {
                    'Year': int(worldCup['Year']),
                    'Host': worldCup['Host']
                }
                worldcupHistoryArray.append(worldCupList)

        populationDouble = float (country['Population'])
        noOfWorldCupWonInt = int (country['No_of_Worldcup_won'])

        db.Country.insert({
            'Country_Name': country['Country_Name'],
            'Capital': country['Capital'],
            'Population': populationDouble,
            'Manager': country['Manager'],
            'No_of_Worldcup_won': noOfWorldCupWonInt,
            'Players': playerArray,
            'WorldCup_History': worldcupHistoryArray
        })


# Create Stadium Document
def createDocumentStadium():
    db = connection.Soccer
    db.Stadium.drop()
    getAllStadium = db.Match_results_Data.distinct('Stadium')
    for stadium in getAllStadium:
        stadiumCity = db.Match_results_Data.find_one({'Stadium': stadium})['Host_city']
        stadiumDetails = db.Match_results_Data.find({'Stadium': stadium})
        matchHistoryArray = []
        for data in stadiumDetails:
            matchList = {
                'Team1': data['Team1'],
                'Team2': data['Team2'],
                'Team1_score': int(data['Team1_score']),
                'Team2_score': int(data['Team2_score']),
                'Date': data['Date']
            }
            matchHistoryArray.append(matchList)

        db.Stadium.insert({
            'Stadium': stadium,
            'City': stadiumCity,
            'Match': matchHistoryArray
        })

if __name__ == '__main__':
    createJson()
    insertIntoDB()
    createDocumentCountry()
    createDocumentStadium()
