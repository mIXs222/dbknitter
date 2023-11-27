import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to get the MySQL data
def get_mysql_data(query, connection_params):
    conn = pymysql.connect(
        host=connection_params['hostname'],
        user=connection_params['username'],
        password=connection_params['password'],
        db=connection_params['database'],
        cursorclass=pymysql.cursors.Cursor
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        conn.close()
    
    return pd.DataFrame(list(result))

# Function to get the MongoDB data
def get_mongodb_data(collection_name, connection_params):
    client = pymongo.MongoClient(
        host=connection_params['hostname'],
        port=connection_params['port']
    )
    db = client[connection_params['database']]
    collection = db[collection_name]

    data = pd.DataFrame(list(collection.find()))
    client.close()

    return data

# Function to get the Redis data
def get_redis_data(table_name, connection_params):
    r = DirectRedis(
        host=connection_params['hostname'],
        port=connection_params['port'],
        db=connection_params['database']
    )
    
    df = pd.read_json(r.get(table_name))
    
    return df

# Query to fetch data from MySQL
mysql_query = """
SELECT s.S_NATIONKEY, p.P_TYPE, ps.PS_SUPPLYCOST 
FROM supplier s 
JOIN partsupp ps ON s.S_SUPPKEY = ps.PS_SUPPKEY
"""
mysql_connection_params = {
    'hostname': 'mysql',
    'username': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Get data from MySQL
mysql_data = get_mysql_data(mysql_query, mysql_connection_params)

# Get data from MongoDB
mongodb_connection_params = {
    'hostname': 'mongodb',
    'port': 27017,
    'database': 'tpch',
}
mongodb_nation_data = get_mongodb_data('nation', mongodb_connection_params)
mongodb_part_data = get_mongodb_data('part', mongodb_connection_params)

# Get data from Redis
redis_connection_params = {
    'hostname': 'redis',
    'port': 6379,
    'database': 0,
}
redis_orders_data = get_redis_data('orders', redis_connection_params)
redis_lineitem_data = get_redis_data('lineitem', redis_connection_params)

# Merge MySQL and MongoDB data
merged_data = mysql_data.merge(mongodb_nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_data = merged_data.merge(mongodb_part_data, left_on='P_TYPE', right_on='P_TYPE')

# Merge Redis data with the rest
complete_data = merged_data.merge(redis_orders_data, how='inner', left_on='S_NATIONKEY', right_on='O_ORDERKEY')
complete_data = complete_data.merge(redis_lineitem_data, on='L_ORDERKEY')

# Calculate the profit
complete_data['PROFIT'] = (complete_data['L_EXTENDEDPRICE'] * (1 - complete_data['L_DISCOUNT'])) - (complete_data['PS_SUPPLYCOST'] * complete_data['L_QUANTITY'])

# Group by nation and year with profit
result = complete_data.groupby(['N_NAME', 'O_ORDERDATE']).agg({'PROFIT': 'sum'}).reset_index()

# Sort the results
result.sort_values(by=['N_NAME', 'O_ORDERDATE'], ascending=[True, False], inplace=True)

# Output to csv
result.to_csv('query_output.csv', index=False)
