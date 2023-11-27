# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# Connection to MySQL
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MySQL for the customers and lineitems data
with mysql_connection.cursor() as cursor:
    query = """
    SELECT c.C_CUSTKEY, c.C_NAME, c.C_ADDRESS, c.C_PHONE, c.C_ACCTBAL, c.C_COMMENT,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue_lost
    FROM customer c
    JOIN lineitem l ON c.C_CUSTKEY = l.L_ORDERKEY
    WHERE l.L_RETURNFLAG = 'R'
    AND l.L_SHIPDATE BETWEEN '1993-10-01' AND '1994-01-01'
    GROUP BY c.C_CUSTKEY
    ORDER BY revenue_lost DESC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL;
    """
    cursor.execute(query)
    mysql_results = cursor.fetchall()

# Query MongoDB for the nations data
nation_data = mongo_db.nation.find()

# Combine nations data into a dictionary for fast access
nations_dict = {doc['N_NATIONKEY']: doc for doc in nation_data}

# Combine MySQL and MongoDB data
combined_results = []
for row in mysql_results:
    nation = nations_dict.get(row[0])  # Assuming C_NATIONKEY maps correctly to N_NATIONKEY
    if nation:
        combined_results.append({
            'Customer Name': row[1],
            'Address': row[2],
            'Nation': nation['N_NAME'],
            'Phone': row[3],
            'Account Balance': row[4],
            'Comment': row[5],
            'Revenue Lost': row[6]
        })

# Writing results to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['Customer Name', 'Address', 'Nation', 'Phone', 'Account Balance', 'Comment', 'Revenue Lost']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in combined_results:
        writer.writerow(data)

# Close both connections
mysql_connection.close()
mongo_client.close()
