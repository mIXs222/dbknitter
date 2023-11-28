import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongodb_client['tpch']

# MySQL queries
mysql_cursor.execute("""
SELECT 
    c.C_CUSTKEY, c.C_NATIONKEY, l.L_EXTENDEDPRICE, l.L_DISCOUNT, o.O_ORDERDATE
FROM 
    customer AS c 
JOIN 
    orders AS o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN 
    lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE 
    o.O_ORDERDATE >= '1990-01-01' AND o.O_ORDERDATE <= '1994-12-31'
""")
mysql_results = mysql_cursor.fetchall()

# MongoDB queries
nation_data = list(mongodb.nation.find({}))
region_data = list(mongodb.region.find({'R_NAME': 'ASIA'}))

# Process and combine data
asia_nation_keys = {n['N_NATIONKEY'] for n in nation_data if n['N_REGIONKEY'] in {r['R_REGIONKEY'] for r in region_data}}
filtered_data = [
    (c_custkey, c_nationkey, l_extendedprice, l_discount, o_orderdate)
    for c_custkey, c_nationkey, l_extendedprice, l_discount, o_orderdate in mysql_results
    if c_nationkey in asia_nation_keys
]

# Calculate revenue and group by nation
revenue_by_nation = {}
for c_custkey, c_nationkey, l_extendedprice, l_discount, o_orderdate in filtered_data:
    revenue = l_extendedprice * (1 - l_discount)
    nation_name = next((n['N_NAME'] for n in nation_data if n['N_NATIONKEY'] == c_nationkey), None)
    if nation_name is not None:
        if nation_name in revenue_by_nation:
            revenue_by_nation[nation_name] += revenue
        else:
            revenue_by_nation[nation_name] = revenue

# Sort revenue by nation in descending order
sorted_revenue_by_nation = sorted(revenue_by_nation.items(), key=lambda item: item[1], reverse=True)

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['NATION', 'REVENUE'])
    for nation, revenue in sorted_revenue_by_nation:
        csvwriter.writerow([nation, revenue])

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongodb_client.close()
