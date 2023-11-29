import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish connection to MySQL database tpch
mysql_connection = pymysql.connect(host='mysql', 
                                   user='root', 
                                   password='my-secret-pw', 
                                   database='tpch')

# Query to select suppliers from MySQL
supplier_query = """
SELECT S_SUPPKEY
FROM supplier
WHERE S_COMMENT NOT LIKE '%Customer%Complaints%'
"""

# Read the query results into a Pandas DataFrame
suppliers_df = pd.read_sql(supplier_query, mysql_connection)
mysql_connection.close()

# Establish a connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client.tpch

# Aggregate pipeline in MongoDB to filter and join the partsupp collection
pipeline = [
    {"$match": {
        "PS_PARTKEY": {"$nin": [45]},
        "PS_SUPPKEY": {"$nin": suppliers_df['S_SUPPKEY'].tolist()},
        "PS_COMMENT": {"$not": {"$regex": "MEDIUM POLISHED"}},
    }},
    {"$group": {
        "_id": {
            "PS_SUPPKEY": "$PS_SUPPKEY",
            "PS_AVAILQTY": "$PS_AVAILQTY",
            "PS_SUPPLYCOST": "$PS_SUPPLYCOST",
        },
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1, "_id.PS_SUPPKEY": 1}}
]

# Execute the aggregate pipeline
partsupp_result = list(mongodb.partsupp.aggregate(pipeline))
mongo_client.close()

# Convert the results to a DataFrame
partsupp_df = pd.DataFrame((d['_id'] for d in partsupp_result), columns=['PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST'])
partsupp_df['count'] = pd.DataFrame([d['count'] for d in partsupp_result])

# Establish connection to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Read part table from Redis into a Pandas DataFrame
part_df = pd.read_json(redis_connection.get('part'))

# Filter parts with the specified sizes and not brand 45
part_df_filtered = part_df[
    part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]) &
    (part_df['P_BRAND'] != 'Brand#45')
]

# Merge the DataFrames (as if performing SQL joins) and final filtering
final_df = (
    partsupp_df.merge(suppliers_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
               .merge(part_df_filtered, how='inner', left_on='PS_SUPPKEY', right_on='P_PARTKEY')
)

# Select the necessary columns for the final output
final_output = final_df[['PS_SUPPLYCOST', 'PS_AVAILQTY', 'count']].sort_values(
    by=['count', 'PS_SUPPLYCOST', 'PS_AVAILQTY'], ascending=[False, True, True]
)

# Write the output to a CSV file
final_output.to_csv('query_output.csv', index=False)
