from pymongo import MongoClient
import mysql.connector
import csv

def run_query():
    # Create MySQL connection
    mydb = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
    )
    
    # Create MongoDB connection
    client = MongoClient('mongodb', 27017)
    mongo_db = client['tpch']

    mycursor = mydb.cursor()

    mycursor.execute("SELECT O_ORDERPRIORITY FROM orders WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'")
    order_data = mycursor.fetchall()
    
    output_data = []
    
    for order in order_data:
        o_orderkey, o_orderpriority = order
        lineitem_collection = mongo_db['lineitem']
        lineitem_count = lineitem_collection.count_documents({'L_ORDERKEY': o_orderkey, 'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'}})
        if lineitem_count > 0:
            output_data.append((o_orderpriority, lineitem_count))

    output_data.sort(key=lambda x: x[0])
    
    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(output_data)

run_query()
