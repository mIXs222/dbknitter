import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Define function to get data from MySQL
def get_mysql_data(conn_info, query):
    connection = pymysql.connect(
        host=conn_info['hostname'],
        user=conn_info['username'],
        password=conn_info['password'],
        db=conn_info['database'],
    )
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
    connection.close()
    return pd.DataFrame(result, columns=columns)

# Define function to get data from MongoDB
def get_mongodb_data(conn_info, collection_name):
    client = pymongo.MongoClient(host=conn_info['hostname'], port=conn_info['port'])
    db = client[conn_info['database']]
    collection = db[collection_name]
    data = list(collection.find())
    client.close()
    return pd.DataFrame(data)

# Define function to get data from Redis
def get_redis_data(conn_info, table_name):
    redis = DirectRedis(host=conn_info['hostname'], port=conn_info['port'], db=conn_info['database'])
    data_str = redis.get(table_name)
    df = pd.read_json(data_str)
    return df

mysql_connection_info = {
    'database': 'tpch',
    'username': 'root',
    'password': 'my-secret-pw',
    'hostname': 'mysql',
}

mongodb_connection_info = {
    'database': 'tpch',
    'port': 27017,
    'hostname': 'mongodb',
}

redis_connection_info = {
    'database': 0,
    'port': 6379,
    'hostname': 'redis',
}

# Connect to MySQL and get nation and supplier tables
nation_query = "SELECT * FROM nation WHERE N_NAME = 'CANADA'"
supplier_query = "SELECT * FROM supplier"
nation_df = get_mysql_data(mysql_connection_info, nation_query)
supplier_df = get_mysql_data(mysql_connection_info, supplier_query)

# Connect to MongoDB and get partsupp and lineitem tables
partsupp_df = get_mongodb_data(mongodb_connection_info, "partsupp")
lineitem_df = get_mongodb_data(mongodb_connection_info, "lineitem")

# Connect to Redis and get part table
part_df = get_redis_data(redis_connection_info, "part")

# Filter lineitem for the given date range and join with partsupp
lineitem_filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= "1994-01-01") & (lineitem_df['L_SHIPDATE'] < "1995-01-01")]
partsupp_lineitem_df = pd.merge(partsupp_df, lineitem_filtered_df, how='inner', on=['PS_SUPPKEY', 'PS_PARTKEY'])

# Calculate the total quantity of parts per supplier
total_qty_per_supplier = partsupp_lineitem_df.groupby('PS_SUPPKEY')['L_QUANTITY'].sum().reset_index()

# Find parts with 'forest' in their names
forest_parts_df = part_df[part_df['P_NAME'].str.contains("forest", case=False, na=False)]

# Find the suppliers who provide these forest parts
suppliers_with_forest_parts = pd.merge(forest_parts_df, partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Calculate suppliers' total quantities for forest parts
forest_parts_qty = suppliers_with_forest_parts.groupby('PS_SUPPKEY')['PS_AVAILQTY'].sum().reset_index()

# Evaluate excess of parts, defined as more than 50% of the quantities shipped
excess_suppliers = pd.merge(total_qty_per_supplier, forest_parts_qty, how='inner', on='PS_SUPPKEY')
excess_suppliers = excess_suppliers[excess_suppliers['PS_AVAILQTY'] > (1.5 * excess_suppliers['L_QUANTITY'])]

# Get the final output by including supplier details
final_output = pd.merge(nation_df, excess_suppliers, how='inner', left_on='N_NATIONKEY', right_on='PS_SUPPKEY')
final_output = pd.merge(final_output, supplier_df, how='inner', on=['S_SUPPKEY', 'N_NATIONKEY'])

# Save results to CSV file
final_output.to_csv('query_output.csv', index=False)
