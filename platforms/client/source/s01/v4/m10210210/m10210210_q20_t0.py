import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connection details
mysql_conn_info = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch"
}

mongodb_conn_info = {
    "host": "mongodb",
    "port": 27017,
}

redis_conn_info = {
    "host": "redis",
    "port": 6379,
    "db": 0
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
cursor = mysql_conn.cursor()

# Select parts from Redis
redis_conn = DirectRedis(**redis_conn_info)
part_df = pd.read_json(redis_conn.get('part'))

# Filter parts by name like 'forest%'
filtered_parts = part_df[part_df['P_NAME'].str.startswith('forest')]

# Get PS_SUPPKEY from MySQL partsupp table
cursor.execute("""
    SELECT PS_SUPPKEY, PS_PARTKEY
    FROM partsupp
    WHERE PS_PARTKEY IN %s
    """, [tuple(filtered_parts['P_PARTKEY'])])
partsupp_records = cursor.fetchall()
partsupp_df = pd.DataFrame(partsupp_records, columns=['PS_SUPPKEY', 'PS_PARTKEY'])

# Get sums from lineitem table for each (L_PARTKEY, L_SUPPKEY) composite pair
cursor.execute("""
    SELECT L_SUPPKEY, L_PARTKEY, SUM(L_QUANTITY) as sum_quantity
    FROM lineitem
    WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'
    GROUP BY L_SUPPKEY, L_PARTKEY
    """)
sums_df = pd.DataFrame(cursor.fetchall(), columns=['L_SUPPKEY', 'L_PARTKEY', 'sum_quantity'])

# Combine the dataframes and filter on the conditions
merged_df = partsupp_df.merge(sums_df, left_on=['PS_PARTKEY', 'PS_SUPPKEY'], right_on=['L_PARTKEY', 'L_SUPPKEY'])
filtered_suppliers = merged_df[merged_df['PS_AVAILQTY'] > 0.5 * merged_df['sum_quantity']]['PS_SUPPKEY']

# Connect to MongoDB and retrieve suppliers and nations
mongo_client = pymongo.MongoClient(**mongodb_conn_info)
mongodb = mongo_client['tpch']
supplier_collection = mongodb['supplier']
suppliers_cursor = supplier_collection.find({"S_SUPPKEY": {"$in": list(filtered_suppliers)}})
suppliers_df = pd.DataFrame(list(suppliers_cursor))

nation_collection = mongodb['nation']
nations_cursor = nation_collection.find({"N_NAME": "CANADA"})
nations_df = pd.DataFrame(list(nations_cursor))

# Join the suppliers with nations
results_df = suppliers_df.merge(nations_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
ordered_results_df = results_df.sort_values(by='S_NAME')[['S_NAME', 'S_ADDRESS']]

# Output to CSV
ordered_results_df.to_csv('query_output.csv', index=False)

# Close all connections
cursor.close()
mysql_conn.close()
mongo_client.close()
