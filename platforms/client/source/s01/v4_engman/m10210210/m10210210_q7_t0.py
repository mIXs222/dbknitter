import pymysql
import pymongo
import pandas as pd
from redis import Redis
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get lineitems from MySQL
query = """
    SELECT
        L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
    FROM
        lineitem
    WHERE
        L_SHIPDATE >= '1995-01-01' AND L_SHIPDATE < '1997-01-01'
"""
lineitems = pd.read_sql(query, con=mysql_connection)

# Add year and calculate revenue
lineitems['L_YEAR'] = lineitems['L_SHIPDATE'].apply(lambda d: datetime.strptime(d, '%Y-%m-%d').year)
lineitems['REVENUE'] = lineitems['L_EXTENDEDPRICE'] * (1 - lineitems['L_DISCOUNT'])

# Get suppliers from MongoDB
suppliers = pd.DataFrame(list(mongo_db.supplier.find({}, {'_id': 0, 'S_SUPPKEY': 1, 'S_NATIONKEY': 1})))

# Get nation keys for India and Japan from MongoDB
nations = pd.DataFrame(list(mongo_db.nation.find({'N_NAME': {'$in': ['INDIA', 'JAPAN']}}, {'_id': 0, 'N_NATIONKEY': 1, 'N_NAME': 1})))

# Get customers from Redis
customers = pd.DataFrame(redis_client.get('customer'))

# Rename nation keys
nations.rename(columns={'N_NATIONKEY': 'nation_key', 'N_NAME': 'nation_name'}, inplace=True)

# Join tables
merged_results = lineitems.merge(suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY') \
                          .merge(customers, left_on='L_ORDERKEY', right_on='C_CUSTKEY') \
                          .merge(nations, left_on='C_NATIONKEY', right_on='nation_key')

# Filter the results according to the nations
filtered_results = merged_results[((merged_results['S_NATIONKEY'] == nations.loc[nations['nation_name'] == 'INDIA', 'nation_key'].values[0]) &
                                   (merged_results['C_NATIONKEY'] == nations.loc[nations['nation_name'] == 'JAPAN', 'nation_key'].values[0])) |
                                  ((merged_results['S_NATIONKEY'] == nations.loc[nations['nation_name'] == 'JAPAN', 'nation_key'].values[0]) &
                                   (merged_results['C_NATIONKEY'] == nations.loc[nations['nation_name'] == 'INDIA', 'nation_key'].values[0]))]

# Select and rename columns
results = filtered_results[['C_NATIONKEY', 'L_YEAR', 'REVENUE', 'S_NATIONKEY']]
results.rename(columns={'C_NATIONKEY': 'CUST_NATION', 'S_NATIONKEY': 'SUPP_NATION'}, inplace=True)

# Sorting results
results_sorted = results.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write to CSV
results_sorted.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close all connections
mysql_connection.close()
mongo_client.close()
