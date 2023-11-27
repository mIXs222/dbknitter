# query.py

import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['Customer Name', 'Address', 'Nation', 'Phone', 'Account Balance', 'Comment', 'Lost Revenue'])

    with mysql_conn.cursor() as cursor:
        # Get the relevant nation data from MongoDB
        nations = {doc['N_NATIONKEY']: doc['N_NAME'] for doc in nation_collection.find({})}

        # SQL query
        query = """
            SELECT
                c.C_NAME,
                c.C_ADDRESS,
                c.C_NATIONKEY,
                c.C_PHONE,
                c.C_ACCTBAL,
                c.C_COMMENT,
                SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS lost_revenue
            FROM
                customer AS c
            JOIN
                orders AS o ON c.C_CUSTKEY = o.O_CUSTKEY
            JOIN
                lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
            WHERE
                l.L_RETURNFLAG = 'R'
                AND o.O_ORDERDATE >= '1993-10-01'
                AND o.O_ORDERDATE < '1994-01-01'
            GROUP BY
                c.C_CUSTKEY
            ORDER BY
                lost_revenue DESC,
                c.C_CUSTKEY,
                c.C_NAME,
                c.C_ACCTBAL
        """
        
        cursor.execute(query)
        results = cursor.fetchall()

        for row in results:
            # Replace nation key with nation name from MongoDB
            nation = nations.get(row[2], 'Unknown')
            csv_writer.writerow([row[0], row[1], nation, row[3], row[4], row[5], row[6]])

# Close the connections
mysql_conn.close()
mongo_client.close()
