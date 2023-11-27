import pymysql
import pymongo
import csv
from pymongo import MongoClient
from datetime import datetime

# Connect to MySQL server
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')
mysql_cursor = mysql_connection.cursor()

# Establish a connection with MongoDB server
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Perform MySQL query
mysql_query = """
    SELECT
        C_NATIONKEY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_ORDERKEY,
        L_SUPPKEY
    FROM
        customer,
        orders,
        lineitem
    WHERE
        C_CUSTKEY = O_CUSTKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_ORDERDATE >= '1990-01-01'
        AND O_ORDERDATE < '1995-01-01'
"""
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()

# Fetch documents from MongoDB collections
nation_documents = mongo_db.nation.find()
region_documents = mongo_db.region.find({'R_NAME': 'ASIA'})
supplier_documents = mongo_db.supplier.find()

# Convert documents to list of dictionaries
nations = list(nation_documents)
regions = list(region_documents)
suppliers = list(supplier_documents)

# Filtering and aggregating data
revenue_by_nation = {}
for result in mysql_results:
    nation_key, extended_price, discount, order_key, supp_key = result
    revenue = extended_price * (1 - discount)
    # Iterate over suppliers to find matching nation
    for supplier in suppliers:
        if supp_key == supplier['S_SUPPKEY'] and supplier['S_NATIONKEY'] == nation_key:
            # Iterate over nations to get the nation name
            for nation in nations:
                if nation_key == nation['N_NATIONKEY']:
                    # Check if the nation belongs to the 'ASIA' region
                    if any(region['R_REGIONKEY'] == nation['N_REGIONKEY'] for region in regions):
                        nation_name = nation['N_NAME']
                        if nation_name not in revenue_by_nation:
                            revenue_by_nation[nation_name] = 0
                        revenue_by_nation[nation_name] += revenue
                        break
            break

# Sort the results by REVENUE DESC
sorted_revenues = sorted(revenue_by_nation.items(), key=lambda x: x[1], reverse=True)

# Write query output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['N_NAME', 'REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for nation_name, revenue in sorted_revenues:
        writer.writerow({'N_NAME': nation_name, 'REVENUE': revenue})

# Close connections
mysql_cursor.close()
mysql_connection.close()
mongo_client.close()
