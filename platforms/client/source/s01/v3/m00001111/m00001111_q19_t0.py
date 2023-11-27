import csv
import mysql.connector
from pymongo import MongoClient

def connect_mysql():
    conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
    cursor = conn.cursor()
    return conn, cursor

def connect_mongodb():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    return db

def mysql_query(cursor):
    query = SELECT P_PARTKEY, P_BRAND, P_CONTAINER, P_SIZE FROM part WHERE P_BRAND IN ('Brand#12', 'Brand#23', 'Brand#34') AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND P_SIZE BETWEEN 1 AND 15
    cursor.execute(query)
    return cursor.fetchall()

def mongodb_query(db):
    records = db.lineitem.find({ '$or' : [ { 'L_PARTKEY' : ..., 'L_QUANTITY' : { '$gte' : 1, '$lte' : 11 }, ... }, { 'L_PARTKEY' : ..., 'L_QUANTITY' : { '$gte' : 10, '$lte' : 20 }, ... }, { 'L_PARTKEY' : ..., 'L_QUANTITY' : { '$gte' : 20, '$lte' : 30 }, ... } ] })
    return records

def write_to_csv(results):
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(results)

conn, cursor = connect_mysql()
db = connect_mongodb()

results_mysql = mysql_query(cursor)
results_mongodb = mongodb_query(db)

results = results_mysql + results_mongodb

write_to_csv(results)
