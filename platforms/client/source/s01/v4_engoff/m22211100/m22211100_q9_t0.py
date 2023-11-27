import pymysql
import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL (orders and lineitem tables)
mysql_query = """
SELECT 
    YEAR(o_orderdate) as o_year, 
    l_extendedprice, 
    l_discount, 
    l_quantity,
    l_orderkey
FROM orders 
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_partkey IN (SELECT P_PARTKEY FROM part WHERE P_NAME LIKE '%dim%');
"""
mysql_cursor.execute(mysql_query)
orders_lineitems_result = mysql_cursor.fetchall()

# Retrieve data from MongoDB (supplier and partsupp)
parts = {part['P_PARTKEY']: part for part in redis_client.get('part')}
suppliers = list(mongodb.supplier.find())
partsupp = list(mongodb.partsupp.find({'PS_PARTKEY': {'$in': list(parts.keys())}}))

# Combine the results from different databases and calculate profit
profit_results = []
for record in orders_lineitems_result:
    o_year, l_extendedprice, l_discount, l_quantity, l_orderkey = record
    for ps in partsupp:
        if ps['PS_PARTKEY'] == l_orderkey:
            for s in suppliers:
                if s['S_SUPPKEY'] == ps['PS_SUPPKEY']:
                    profit = (l_extendedprice * (1 - l_discount)) - (ps['PS_SUPPLYCOST'] * l_quantity)
                    profit_results.append((s['S_NATIONKEY'], o_year, profit))
                    break

# Transform the results to a DataFrame
profit_df = pd.DataFrame(profit_results, columns=['nation', 'year', 'profit'])
aggregated_results = profit_df.groupby(['nation', 'year']).agg({'profit': 'sum'}).reset_index()

# Sort the results
aggregated_results.sort_values(by=['nation', 'year'], ascending=[True, False], inplace=True)

# Write to CSV
aggregated_results.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close database connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
