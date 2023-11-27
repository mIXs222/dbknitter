from pymongo import MongoClient
import csv
import mysql.connector

#connecting with mongodb
client = MongoClient('mongodb', port=27017)
db = client['tpch'] #dbname
collection = db['lineitem'] #tablename

#querying the mongodb & extracting data
query = {"L_SHIPDATE":{'$gte':'1994-01-01','$lt':'1995-01-01'}, "L_DISCOUNT":{'$gte':.06 - 0.01,'$lte':.06 + 0.01}, "L_QUANTITY":{'$lt':24}}
projection = {"L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1}
mongo_data = collection.find(query, projection)

#performing the summation operation
result = sum([data['L_EXTENDEDPRICE'] * data['L_DISCOUNT'] for data in mongo_data])

# assuming we have found a connection to mysql with a cursor named 'cursor' (will be shown in the next part!)
# writing results into csv
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["REVENUE"])
    writer.writerow([result])
