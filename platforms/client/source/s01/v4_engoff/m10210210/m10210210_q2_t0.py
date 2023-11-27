import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and fetching region and partsupp information

# MySQL connection setup
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# MySQL query for the Europe region
mysql_query_region = """
SELECT R_REGIONKEY FROM region WHERE R_NAME='EUROPE';
"""

# Connect to MySQL and fetch data
mysql_conn = pymysql.connect(**mysql_conn_info)
with mysql_conn:
    with mysql_conn.cursor() as cursor:
        cursor.execute(mysql_query_region)
        europe_region_keys = [row[0] for row in cursor.fetchall()]

    # Fetch partsupp information that matches parts of brass type and size 15
    partsupp_query = """
    SELECT PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST FROM partsupp
    WHERE PS_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_TYPE='BRASS' AND P_SIZE=15);
    """
    cursor.execute(partsupp_query)
    partsupp_data = cursor.fetchall()

mysql_conn.close()

# Convert partsupp_data to DataFrame
partsupp_df = pd.DataFrame(partsupp_data, columns=['P_PARTKEY', 'S_SUPPKEY', 'PS_SUPPLYCOST'])


# MongoDB connection and fetching nation and supplier

# MongoDB client setup
mongodb_conn_info = {
    'host': 'mongodb',
    'port': 27017,
}

mongodb_db_name = 'tpch'
# MongoDB query for nation and supplier
client = pymongo.MongoClient(**mongodb_conn_info)
mongodb = client[mongodb_db_name]

# Get nation data
nations = mongodb.nation.find({'N_REGIONKEY': {'$in': europe_region_keys}})
nations_df = pd.DataFrame(list(nations))


# Get supplier data
suppliers = mongodb.supplier.find()
suppliers_df = pd.DataFrame(list(suppliers))

client.close()


# Redis connection and fetching part information

# Redis connection setup
redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0,
}

# Connect to Redis
redis = DirectRedis(**redis_conn_info)

# Get part data
part_df = pd.DataFrame(redis.get('part'))

redis.close()


# Performing the joins and filter conditions

# Filter suppliers from the Europe nation
europe_suppliers_df = suppliers_df[suppliers_df['S_NATIONKEY'].isin(nations_df['N_NATIONKEY'])]

# Merge partsupp and part, then filter parts of brass type and size 15
parts_df = partsupp_df.merge(part_df, left_on='P_PARTKEY', right_on='P_PARTKEY')
parts_df = parts_df[(parts_df['P_TYPE'] == 'BRASS') & (parts_df['P_SIZE'] == 15)]

# Calculate the minimum cost for each part
min_cost_df = parts_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()

# Find suppliers that offer the min cost
min_cost_suppliers_df = parts_df.merge(
    min_cost_df, 
    left_on=['P_PARTKEY', 'PS_SUPPLYCOST'], 
    right_on=['P_PARTKEY', 'PS_SUPPLYCOST']
)

# Merge min cost suppliers with supplier and nation information 
final_df = min_cost_suppliers_df.merge(
    europe_suppliers_df, 
    left_on='S_SUPPKEY', 
    right_on='S_SUPPKEY'
).merge(
    nations_df, 
    left_on='S_NATIONKEY', 
    right_on='N_NATIONKEY'
)

# Select and rename columns for the final output
output_columns = [
    'S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY',
    'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT'
]
final_df = final_df[output_columns]
final_df.columns = [
    'Supplier Account Balance', 'Supplier Name', 'Nation Name', 'Part Key', 
    'Part Manufacturer', 'Supplier Address', 'Supplier Phone', 'Supplier Comment'
]

# Sort the DataFrame
final_df = final_df.sort_values(by=[
    'Supplier Account Balance', 'Nation Name', 'Supplier Name', 'Part Key'
], ascending=[False, True, True, True])


# Write the result to a CSV file
final_df.to_csv('query_output.csv', index=False)
