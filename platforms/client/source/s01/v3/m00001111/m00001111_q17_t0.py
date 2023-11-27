import csv
import mysql.connector
import pymongo

def get_data_from_mysql(query):
    cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql',
                              database='tpch')
    cursor = cnx.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    cnx.close()
    return data

def get_data_from_mongo(query):
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = client["tpch"]
    data = db["lineitem"].find(query)
    client.close()
    return data

mysql_query = "SELECT * FROM part WHERE P_BRAND = 'Brand#23' AND P_CONTAINER = 'MED BAG'"
part_data = get_data_from_mysql(mysql_query)

avg_query = {"$group" : { "_id" : "L_PARTKEY", "avg_quantity" : { "$avg" : "$L_QUANTITY" } } }
avg_quantity_data = get_data_from_mongo(avg_query)

result = []
for pd in part_data:
    for avg_qd in avg_quantity_data:
        if pd['P_PARTKEY'] == avg_qd['_id'] and pd['P_QUANTITY'] < 0.2*avg_qd['avg_quantity']:
            result.append(pd['P_EXTENDEDPRICE']/7.0)

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(result)

