import csv
from pymongo import MongoClient
import mysql.connector

# Connect to MongoDB server
client = MongoClient("mongodb://localhost:27017/")
mongodb = client["tpch"]

# Connecting to MySQL
mydb = mysql.connector.connect(host="localhost", user="your_username", password="your_password", database="tpch")
mycursor = mydb.cursor()

supplier_col = mongodb["supplier"].find({}, {"_id": 0, "S_SUPPKEY": 1, "S_NAME": 1, "S_ADDRESS": 1, "S_NATIONKEY": 1})

supplier_list = [data for data in supplier_col]

for supplier in supplier_list:
    partsupp_query = f"SELECT PS_SUPPKEY FROM partsupp WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM PART WHERE P_NAME LIKE 'forest%') AND PS_AVAILQTY > (SELECT 0.5 * SUM(L_QUANTITY) FROM lineitem WHERE L_PARTKEY = {supplier['S_SUPPKEY']} AND L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01')"
    mycursor.execute(partsupp_query)
    results = mycursor.fetchall()

    for result in results:
        if (result[0] == supplier['S_SUPPKEY']):
            nation_query = f"SELECT N_NAME FROM nation WHERE N_NATIONKEY = {supplier['S_NATIONKEY']}"
            mycursor.execute(nation_query)
            nations = mycursor.fetchall()

            for nation in nations:
                if (nation[0] == 'CANADA'):
                    with open('query_output.csv', mode='w') as file:
                        writer = csv.writer(file)
                        writer.writerow(['S_NAME', 'S_ADDRESS'])
                        writer.writerow([supplier['S_NAME'], supplier['S_ADDRESS']])
