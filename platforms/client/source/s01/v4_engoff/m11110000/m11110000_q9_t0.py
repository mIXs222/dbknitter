# query.py
import pymysql
import pymongo
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client['tpch']

# Function to convert MongoDB cursor to list of dictionaries
def mongo_cursor_to_dicts(cursor):
    return list(map(lambda doc: doc, cursor))

# Retrieve data from MySQL
with mysql_conn.cursor() as cursor:
    mysql_query = """
        SELECT
            s.s_nationkey,
            EXTRACT(YEAR FROM o.o_orderdate) AS o_year,
            (l.l_extendedprice * (1 - l.l_discount)) - (ps.ps_supplycost * l.l_quantity) AS profit
        FROM
            orders o, lineitem l, partsupp ps, supplier s
        WHERE
            o.o_orderkey = l.l_orderkey
            AND l.l_partkey = ps.ps_partkey
            AND l.l_suppkey = ps.ps_suppkey
            AND ps.ps_suppkey = s.s_suppkey
    """
    cursor.execute(mysql_query)
    mysql_data = cursor.fetchall()

# Retrieve data from MongoDB
mongo_data = {}
for nation in mongo_cursor_to_dicts(mongodb.nation.find({}, {'_id': 0})):
    nation_key = nation['N_NATIONKEY']
    mongo_data[nation_key] = {
        'nation': nation['N_NAME'],
        'profit_data': []
    }   

# Merging MySQL data with MongoDB nation data
for (nationkey, year, profit) in mysql_data:
    if nationkey in mongo_data:
        mongo_data[nationkey]['profit_data'].append({
            'year': year,
            'profit': profit
        })

# Prepare final data for CSV
final_data = []
for nationkey, data in mongo_data.items():
    for profit_data in data['profit_data']:
        final_data.append({
            'nation': data['nation'],
            'year': profit_data['year'],
            'profit': profit_data['profit']
        })

# Sorting the final data
final_data.sort(key=lambda x: (-x['year'], x['nation']))

# Write output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['nation', 'year', 'profit']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in final_data:
        row['year'] = int(row['year']) # Convert year to int for proper formatting
        writer.writerow(row)

# Close the connections
mysql_conn.close()
mongo_client.close()
