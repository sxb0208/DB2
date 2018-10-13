# querying_with_mongodb

##Execute the following queries using the flat relational database.

1) Retrieve the list of country names that have won a world cup.

db.users.find({“Country_Name”:1, “No_of_Worldcup_won”:1},{“No_of_Worldcup_won”:{$gt:0}})

2) Retrieve the list of country names that have won a world cup and the number of world cup each has won in descending order.

3) List the Capital of the countries in increasing order of country population for countries that have population more than 100 million.

4) List the Name of the stadium which has hosted a match where the no of goals scored by a single team was greater than 4.  

5) List the names of all the cities which have the name of the Stadium starting with “Estadio”.

6) List all stadiums and the number of matches hosted by each stadium.

7) List the First Name,Last Name and Date of Birth of Players whose heights are greater than 198 cms.
