import pymysql
import pymongo
import csv
from datetime import datetime

# Mysql connection setup
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Mongodb connection setup
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# SQL Query to fetch relevant data from mysql
mysql_query = """
SELECT 
    c.C_NAME AS customer_name,
    c.C_NATIONKEY AS customer_nation_key,
    s.S_NATIONKEY AS supplier_nation_key,
    YEAR(l.L_SHIPDATE) AS year,
    l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS revenue
FROM 
    customer c, 
    supplier s, 
    lineitem l
WHERE 
    c.C_CUSTKEY = l.L_SUPPKEY AND 
    s.S_SUPPKEY = l.L_SUPPKEY AND 
    l.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""

# Function to create nation dictionary from mongodb
def get_nation_dict():
    nations_coll = mongo_db.nation.find()
    nation_dict = {}
    for nation in nations_coll:
        nation_dict[nation['N_NATIONKEY']] = nation['N_NAME']
    return nation_dict

nation_dict = get_nation_dict()

# Keys for "INDIA" and "JAPAN" from nation dictionary
india_nation_key, japan_nation_key = None, None
for key, value in nation_dict.items():
    if value == 'INDIA':
        india_nation_key = key
    if value == 'JAPAN':
        japan_nation_key = key

# Execute the mysql query and process data
with mysql_connection.cursor() as cursor:
    cursor.execute(mysql_query)
    results = cursor.fetchall()
    # Filter and transform results based on nations
    results_to_write = [
        {
            "CUST_NATION": nation_dict[row[1]],
            "L_YEAR": row[3],
            "REVENUE": row[4],
            "SUPP_NATION": nation_dict[row[2]],
        }
        for row in results
        if (row[1] == india_nation_key and row[2] == japan_nation_key) or
           (row[1] == japan_nation_key and row[2] == india_nation_key)
    ]

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=["CUST_NATION", "L_YEAR", "REVENUE", "SUPP_NATION"])
    writer.writeheader()
    writer.writerows(results_to_write)

# Close connections
mysql_connection.close()
mongo_client.close()
