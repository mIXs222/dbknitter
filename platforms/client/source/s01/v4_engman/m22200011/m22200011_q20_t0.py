import pymysql
import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# MySQL Query: Get suppliers and partsupp information
mysql_query = """
SELECT
    s.S_SUPPKEY, s.S_NAME, p.PS_PARTKEY, p.PS_AVAILQTY
FROM
    supplier as s
JOIN
    partsupp as p
ON
    s.S_SUPPKEY = p.PS_SUPPKEY;
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    suppliers_partsupp = cursor.fetchall()

# Convert suppliers and partsupp query results to DataFrame
suppliers_partsupp_df = pd.DataFrame(suppliers_partsupp, columns=["S_SUPPKEY", "S_NAME", "PS_PARTKEY", "PS_AVAILQTY"])

# MongoDB Query: Get lineitem information
mongo_query = {
    'L_SHIPDATE': {
        '$gte': '1994-01-01', 
        '$lt': '1995-01-01'
    }
}
lineitem_cursor = mongo_db['lineitem'].find(mongo_query)
lineitems = list(lineitem_cursor)

# Convert lineitem query results to DataFrame
lineitems_df = pd.DataFrame(lineitems)

# Redis data retrieval: Get part information
part_keys = redis_client.keys('part*')
part_dict_list = [eval(redis_client.get(key)) for key in part_keys if key.decode("utf-8").startswith('part:')]
parts_df = pd.DataFrame(part_dict_list)

# Combine the data from different databases
combined_data = suppliers_partsupp_df.merge(lineitems_df, how='inner', left_on=['S_SUPPKEY', 'PS_PARTKEY'], right_on=['L_SUPPKEY', 'L_PARTKEY'])
combined_data = combined_data.merge(parts_df, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')
combined_data = combined_data[combined_data['P_NAME'].str.contains('forest', case=False)]

# Filter combined_data for parts shipped for CANADA and having excess quantity
# Assuming nation data containing relation between S_NATIONKEY and the country name is present and fetched from Redis.
nation_df = pd.DataFrame(eval(redis_client.get('nation')), columns=['N_NATIONKEY', 'N_NAME'])
combined_data = combined_data.merge(nation_df, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
combined_data = combined_data[combined_data['N_NAME'] == 'CANADA']
combined_data['EXCESS_QTY'] = combined_data['PS_AVAILQTY'] > (0.5 * combined_data['L_QUANTITY'])

# Export result to query_output.csv
result_df = combined_data[combined_data['EXCESS_QTY']][['S_SUPPKEY', 'S_NAME', 'PS_PARTKEY', 'PS_AVAILQTY']]
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)

# Close connections
mysql_conn.close()
mongo_client.close()
