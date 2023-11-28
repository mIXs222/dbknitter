import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# Define connection parameters for MySQL
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Define connection parameters for MongoDB
mongodb_conn_info = {
    'host': 'mongodb',
    'port': 27017
}

# Define connection parameters for Redis
redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
cursor = mysql_conn.cursor()

# Query MySQL for suppliers in Canada
query_nation = """
SELECT N_NATIONKEY
FROM nation
WHERE N_NAME = 'CANADA'
"""
cursor.execute(query_nation)
nation_keys = [nation[0] for nation in cursor.fetchall()]

query_suppliers = f"""
SELECT S_SUPPKEY, S_NAME, S_ADDRESS
FROM supplier
WHERE S_NATIONKEY IN {tuple(nation_keys)}
"""
cursor.execute(query_suppliers)
suppliers = cursor.fetchall()

mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient(**mongodb_conn_info)
mongodb = mongo_client.tpch

# Query MongoDB for parts starting with 'forest'
parts_query = {'P_NAME': {'$regex': '^forest', '$options': 'i'}}
part_keys = [part['P_PARTKEY'] for part in mongodb.part.find(parts_query, {'_id': 0, 'P_PARTKEY': 1})]

# Connect to Redis
redis_conn = direct_redis.DirectRedis(**redis_conn_info)

# Query Redis for partsupp to filter suppliers based on parts
partsupp_keys = pd.DataFrame(redis_conn.get('partsupp')).query('PS_PARTKEY in @part_keys')['PS_SUPPKEY'].unique().tolist()

# Query Redis for lineitem and calculate threshold quantity
date_format = "%Y-%m-%d"
start_date = datetime.strptime('1994-01-01', date_format)
end_date = datetime.strptime('1995-01-01', date_format)
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))
lineitem_df_filtered = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] <= end_date) &
    (lineitem_df['L_PARTKEY'].isin(part_keys)) &
    (lineitem_df['L_SUPPKEY'].isin(partsupp_keys))
]
threshold_quantities = lineitem_df_filtered.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].transform('sum') / 2
lineitem_df_filtered = lineitem_df_filtered[lineitem_df_filtered['L_QUANTITY'] >= threshold_quantities]

# Prepare final output
final_suppliers = {s[0]: (s[1], s[2]) for s in suppliers if s[0] in lineitem_df_filtered['L_SUPPKEY'].unique()}

# Output to CSV file
output = [(name, address) for suppkey, (name, address) in sorted(final_suppliers.items(), key=lambda x: x[1])]
output_df = pd.DataFrame(output, columns=['S_NAME', 'S_ADDRESS'])
output_df.to_csv('query_output.csv', index=False)
