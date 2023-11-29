import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to run query on MySQL
def query_mysql():
    # Connect to MySQL
    mysql_conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        cursorclass=pymysql.cursors.Cursor
    )
    try:
        with mysql_conn.cursor() as cursor:
            # Select Canadian suppliers and their parts
            sql = """
            SELECT s.S_SUPPKEY, n.N_NAME
            FROM supplier s JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
            WHERE n.N_NAME = 'CANADA';
            """
            cursor.execute(sql)
            suppliers = cursor.fetchall()

            # Convert to DataFrame
            suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'N_NAME'])
    finally:
        mysql_conn.close()
    return suppliers_df

# Function to get collection from MongoDB
def get_mongodb_collection():
    # Connect to MongoDB
    mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = mongo_client["tpch"]
    return db["part"]

# Function to run query on Redis and get partsupp DataFrame
def get_redis_dataframe():
    # Connect to Redis
    redis_conn = DirectRedis(host='redis', port=6379, db=0)
    # Get 'partsupp' as DataFrame
    partsupp_df = pd.read_json(redis_conn.get('partsupp'))
    return partsupp_df

# Get the data from MySQL and MongoDB
suppliers_df = query_mysql()
part_collection = get_mongodb_collection()

# Query parts from MongoDB
forest_parts_cursor = part_collection.find({'P_NAME': {'$regex': '^forest.*', '$options': 'i'}})
forest_parts_df = pd.DataFrame(list(forest_parts_cursor))
forest_parts_df = forest_parts_df.rename(columns={'_id': 'P_PARTKEY'})

# Get the data from Redis
partsupp_df = get_redis_dataframe()

# Combining data
combined_df = suppliers_df.merge(partsupp_df, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
forest_df = combined_df[combined_df['PS_PARTKEY'].isin(forest_parts_df['P_PARTKEY'])]

# Calculate the excess quantity
forest_df_grouped = forest_df.groupby('S_SUPPKEY').agg({'PS_AVAILQTY': 'sum'}).reset_index()
forest_df_grouped = forest_df_grouped[forest_df_grouped['PS_AVAILQTY'] > forest_parts_df['P_SIZE'].sum() * 0.5]

# Final output
output_df = forest_df_grouped[['S_SUPPKEY', 'PS_AVAILQTY']]
output_df.to_csv('query_output.csv', index=False)
