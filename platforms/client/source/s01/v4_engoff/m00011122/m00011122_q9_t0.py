# python_code.py

import pandas as pd
import pymysql
from pymongo import MongoClient
import direct_redis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# DirectRedis connection
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Define the part name to search parts for
specified_dim = 'SPECIFIED PART NAME TO BE DETERMINED'

# Fetch data from MySQL
with mysql_conn.cursor() as cursor:
    # Fetch nation data
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
    nations = cursor.fetchall()

    # Fetch part data (only where P_NAME contains the specified dim)
    query = f"SELECT P_PARTKEY FROM part WHERE P_NAME LIKE '%{specified_dim}%'"
    cursor.execute(query)
    parts = cursor.fetchall()
    part_keys = [part[0] for part in parts]

# Fetch data from MongoDB
suppliers = list(mongo_db.supplier.find({}, {'S_SUPPKEY': 1, 'S_NATIONKEY': 1}))
partsupp = list(mongo_db.partsupp.find({'PS_PARTKEY': {'$in': part_keys}}, {'PS_PARTKEY': 1, 'PS_SUPPKEY': 1, 'PS_SUPPLYCOST': 1}))

# Mapping suppliers to nations and finding relevant partsupps
supplier_nation_mapping = {s['S_SUPPKEY']: s['S_NATIONKEY'] for s in suppliers}
relevant_partsupps = [ps for ps in partsupp if ps['PS_SUPPKEY'] in supplier_nation_mapping]

# Fetch data from Redis
lineitems_df = pd.DataFrame(redis_conn.get('lineitem'))

# Filter lineitems with relevant part keys
lineitems_df = lineitems_df[lineitems_df['L_PARTKEY'].isin(part_keys)]
lineitems_df['year'] = pd.to_datetime(lineitems_df['L_SHIPDATE']).dt.year

# Merging data to calculate profit
profits = []
for lineitem in lineitems_df.itertuples():
    ps = next((ps for ps in relevant_partsupps if ps['PS_PARTKEY'] == lineitem.L_PARTKEY and ps['PS_SUPPKEY'] == lineitem.L_SUPPKEY), None)
    if ps:
        nation_key = supplier_nation_mapping[ps['PS_SUPPKEY']]
        nation = next((n for n in nations if n[0] == nation_key), None)
        if nation:
            profit = (lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) - (ps['PS_SUPPLYCOST'] * lineitem.L_QUANTITY)
            profits.append({'nation': nation[1], 'year': lineitem.year, 'profit': profit})

# Convert profits to a DataFrame
profit_df = pd.DataFrame(profits)

# Aggregate profit by nation and year
result_df = profit_df.groupby(['nation', 'year']).agg({'profit': 'sum'}).reset_index()

# Sort the results as required
result_df.sort_values(by=['nation', 'year'], ascending=[True, False], inplace=True)

# Write results to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Close the MySQL connection
mysql_conn.close()
