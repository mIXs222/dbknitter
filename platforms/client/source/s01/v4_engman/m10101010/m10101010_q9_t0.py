import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query to fetch data from MySQL 'supplier' and 'lineitem' tables
mysql_query = """
SELECT s.S_NATIONKEY, s.S_NAME, l.L_ORDERKEY, l.L_QUANTITY, l.L_EXTENDEDPRICE, l.L_DISCOUNT, l.L_SHIPDATE
FROM lineitem l
INNER JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
"""

# Query to fetch data from MongoDB 'partsupp' table
mongo_partsupp = mongo_db['partsupp']

# Execute the MySQL query and fetch all results
mysql_cursor.execute(mysql_query)
lineitem_supplier_data = mysql_cursor.fetchall()

# Closing MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Process fetched data and calculate profit
profit_by_nation_and_year = {}

for row in lineitem_supplier_data:
    nationkey, _, orderkey, l_quantity, l_extendedprice, l_discount, l_shipdate = row
    year = datetime.strptime(l_shipdate, '%Y-%m-%d').year
    mongo_partsupp_query = {'PS_PARTKEY': orderkey}
    partsupp_docs = mongo_partsupp.find(mongo_partsupp_query)

    for partsupp_doc in partsupp_docs:
        profit = (l_extendedprice * (1 - l_discount)) - (partsupp_doc['PS_SUPPLYCOST'] * l_quantity)
        if nationkey not in profit_by_nation_and_year:
            profit_by_nation_and_year[nationkey] = {}
        if year not in profit_by_nation_and_year[nationkey]:
            profit_by_nation_and_year[nationkey][year] = profit
        else:
            profit_by_nation_and_year[nationkey][year] += profit

# Write the results to 'query_output.csv'
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['NATION', 'YEAR', 'PROFIT'])

    for nation in sorted(profit_by_nation_and_year):
        sorted_years = sorted(profit_by_nation_and_year[nation], reverse=True)
        for year in sorted_years:
            csvwriter.writerow([nation, year, profit_by_nation_and_year[nation][year]])
