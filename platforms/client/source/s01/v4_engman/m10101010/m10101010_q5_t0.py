import pymysql
import pymongo
import csv
from datetime import datetime

# Establish MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Establish MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Function to execute the MySQL part of the query
def get_mysql_data():
    mysql_query = """
    SELECT nation.N_NAME, SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS REVENUE
    FROM supplier, lineitem, customer, nation
    WHERE lineitem.L_SUPPKEY = supplier.S_SUPPKEY
      AND lineitem.L_ORDERKEY IN (SELECT O_ORDERKEY FROM orders WHERE orders.O_ORDERDATE >= '1990-01-01' AND orders.O_ORDERDATE < '1995-01-01')
      AND customer.C_CUSTKEY = lineitem.L_ORDERKEY
      AND customer.C_NATIONKEY = nation.N_NATIONKEY
      AND supplier.S_NATIONKEY = nation.N_NATIONKEY
      AND nation.N_REGIONKEY IN (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA')
    GROUP BY nation.N_NAME
    ORDER BY REVENUE DESC;
    """
    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query)
        result = cursor.fetchall()
    return result

# Function to get Asian nation keys from MongoDB
def get_asian_nation_keys():
    asian_nations = mongo_db.nation.find({"N_REGIONKEY": {"$in": mongo_db.region.distinct("R_REGIONKEY", {"R_NAME": "ASIA"})}})
    return [n['N_NATIONKEY'] for n in asian_nations]

# Get the qualifying Asian nation keys
asian_nation_keys = get_asian_nation_keys()

# Execute the query on MySQL and fetch the result
mysql_data = get_mysql_data()

# Write the output to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['N_NAME', 'REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in mysql_data:
        if row[0] in asian_nation_keys:
            writer.writerow({'N_NAME': row[0], 'REVENUE': row[1]})

# Close the connections
mysql_conn.close()
mongo_client.close()
