import pymysql
import pymongo
import csv
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
nation_collection = mongo_db['nation']
part_collection = mongo_db['part']
orders_collection = mongo_db['orders']

# Get nation and region key mappings for nation in ASIA and INDIA specifics
nation_keys_asia = []
nation_keys_india = []
for nation in nation_collection.find({"N_REGIONKEY": {"$eq": 1}}):
    nation_keys_asia.append(nation["N_NATIONKEY"])
    if nation['N_NAME'] == 'INDIA':
        nation_keys_india.append(nation["N_NATIONKEY"])

# Get part keys for SMALL PLATED COPPER
part_keys = []
for part in part_collection.find({"P_TYPE": "SMALL PLATED COPPER"}):
    part_keys.append(part["P_PARTKEY"])

# Query MySQL for revenue from products of SMALL PLATED COPPER by suppliers from INDIA
revenue_1995 = 0
revenue_1996 = 0
total_revenue_1995 = 0
total_revenue_1996 = 0

query = """
SELECT
    YEAR(L_SHIPDATE) as year, 
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,
    S_NATIONKEY
FROM
    lineitem, supplier
WHERE
    lineitem.L_SUPPKEY = supplier.S_SUPPKEY
    AND supplier.S_NATIONKEY IN (%s)
    AND lineitem.L_PARTKEY IN (%s)
    AND (YEAR(L_SHIPDATE) = 1995 OR YEAR(L_SHIPDATE) = 1996)
GROUP BY
    YEAR(L_SHIPDATE), S_NATIONKEY;
"""

format_strings = ','.join(['%s'] * len(nation_keys_asia))
format_strings_parts = ','.join(['%s'] * len(part_keys))
query_formatted = query % (format_strings, format_strings_parts)

mysql_cursor.execute(query_formatted, nation_keys_asia + part_keys)

for row in mysql_cursor:
    year, revenue, nation_key = row
    if nation_key in nation_keys_india:
        if year == 1995:
            revenue_1995 += revenue
        elif year == 1996:
            revenue_1996 += revenue
    if year == 1995:
        total_revenue_1995 += revenue
    elif year == 1996:
        total_revenue_1996 += revenue

# Calculating market shares
market_share_1995 = revenue_1995 / total_revenue_1995 if total_revenue_1995 else 0
market_share_1996 = revenue_1996 / total_revenue_1996 if total_revenue_1996 else 0

# Write results to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["YEAR", "MARKET_SHARE"])
    writer.writerow([1995, market_share_1995])
    writer.writerow([1996, market_share_1996])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
