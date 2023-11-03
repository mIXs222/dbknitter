import mysql.connector
import pymongo
import csv
from pymongo import MongoClient
from datetime import datetime 
from decimal import Decimal
from bson.decimal128 import Decimal128

def mysql_connection():
    cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                                  host='mysql',
                                  database='tpch')
    return cnx

def mongodb_connection():
    client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = client["tpch"]
    return db

def execute_query():
    cnx = mysql_connection()
    cursor = cnx.cursor()

    db = mongodb_connection()
    supplier = db['supplier']
    customer = db['customer']
    lineitem = db['lineitem']
    region = db['region']

    query = ("SELECT N_NAME FROM NATION")
    cursor.execute(query)
    nations = cursor.fetchall()

    result = []

    for nation in nations:
        s_nationkey = nation[0]
        suppliers = supplier.find({"S_NATIONKEY": s_nationkey})
        for s in suppliers:
            supkey = s["S_SUPPKEY"]
            items = lineitem.find({"L_SUPPKEY": supkey})
            for item in items:
                custkey = item["C_CUSTKEY"]
                customers = customer.find({"C_CUSTKEY": custkey})
                for c in customers:
                    if c["C_NATIONKEY"] == s_nationkey:
                        revenue = item["L_EXTENDEDPRICE"] * (1 - item["L_DISCOUNT"])
                        regionkey = int(nation[2])
                        regions = region.find({"R_REGIONKEY": regionkey})
                        for r in regions:
                            if r["R_NAME"] == 'ASIA' and item["O_ORDERDATE"] >= datetime.strptime('1990-01-01', "%Y-%m-%d") and item["O_ORDERDATE"] < datetime.strptime('1995-01-01', "%Y-%m-%d"):
                                result.append([nation[1], revenue])

        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(result)

        cnx.close()
