# save as query.py
import pymysql
import pymongo
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]

# Get the N_NATIONKEY for SAUDI ARABIA from MongoDB
nation_collection = mongodb_db["nation"]
saudi_nation = nation_collection.find_one({"N_NAME": "SAUDI ARABIA"})
saudi_nation_key = saudi_nation["N_NATIONKEY"] if saudi_nation else None

if saudi_nation_key is not None:
    # Query suppliers in MySQL that match the SAUDI ARABIA nation key
    mysql_query = """
        SELECT DISTINCT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT
        FROM supplier s
        JOIN orders o ON s.S_SUPPKEY IN (
            SELECT l.L_SUPPKEY
            FROM lineitem l
            WHERE l.L_ORDERKEY = o.O_ORDERKEY AND l.L_RECEIPTDATE > l.L_COMMITDATE
        )
        WHERE o.O_ORDERSTATUS = 'F' AND s.S_NATIONKEY = %s
    """

    # Execute the MySQL query
    mysql_cursor.execute(mysql_query, (saudi_nation_key, ))
    suppliers = mysql_cursor.fetchall()

    # Write result to a CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
        for supplier in suppliers:
            writer.writerow(supplier)

# Close the connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
