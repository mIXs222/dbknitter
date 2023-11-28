import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to retrieve suppliers and nations from MySQL
def get_suppliers_nations():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_NATIONKEY, s.S_PHONE, s.S_ACCTBAL, s.S_COMMENT, n.N_NAME
                FROM supplier s
                JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
                JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
                WHERE r.R_NAME = 'EUROPE'
            """
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'N_NAME'])
    finally:
        connection.close()
    return df

# Function to retrieve parts from MongoDB
def get_parts():
    client = pymongo.MongoClient(host='mongodb', port=27017)
    db = client['tpch']
    parts_cursor = db.part.find({"P_SIZE": 15, "P_TYPE": {"$regex": "BRASS"}}, {"_id": 0})
    df = pd.DataFrame(list(parts_cursor))
    client.close()
    return df

# Function to retrieve partsupp from Redis and convert to DataFrame
def get_partsupp():
    direct_redis = DirectRedis(host='redis', port=6379, db=0)
    partsupp_df = pd.read_json(direct_redis.get('partsupp'), orient='records')
    return partsupp_df

# Retrieve data from different data sources
suppliers_nations_df = get_suppliers_nations()
parts_df = get_parts()
partsupp_df = get_partsupp()

# Merge the dataframes to get the final query result
merged_df = pd.merge(partsupp_df, parts_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = pd.merge(merged_df, suppliers_nations_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Filter by the minimum PS_SUPPLYCOST within each 'S_NATIONKEY'
idx = merged_df.groupby(['S_NATIONKEY'])['PS_SUPPLYCOST'].idxmin()
final_df = merged_df.loc[idx]

# Sort as specified in the query
final_df = final_df.sort_values(['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Write results to CSV file
final_df.to_csv('query_output.csv', index=False)
