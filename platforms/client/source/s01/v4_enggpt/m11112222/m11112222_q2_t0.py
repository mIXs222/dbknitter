import pandas as pd
from pymongo import MongoClient
import direct_redis

# Function to connect to MongoDB and execute query
def get_mongo_data():
    client = MongoClient('mongodb', 27017)
    db = client.tpch

    # Aggregating data from MongoDB
    aggregation_pipeline = [
        {
            '$lookup': {
                'from': 'nation',
                'localField': 'S_NATIONKEY',
                'foreignField': 'N_NATIONKEY',
                'as': 'nation_info'
            }
        },
        {
            '$unwind': '$nation_info'
        },
        {
            '$lookup': {
                'from': 'region',
                'localField': 'nation_info.N_REGIONKEY',
                'foreignField': 'R_REGIONKEY',
                'as': 'region_info'
            }
        },
        {
            '$unwind': '$region_info'
        },
        {
            '$match': {
                'region_info.R_NAME': 'EUROPE'
            }
        },
        {
            '$project': {
                'S_SUPPKEY': 1,
                'S_ACCTBAL': 1,
                'S_NAME': 1,
                'S_ADDRESS': 1,
                'S_PHONE': 1,
                'S_COMMENT': 1,
                'N_NAME': '$nation_info.N_NAME'
            }
        }
    ]

    suppliers_df = pd.DataFrame(list(db.supplier.aggregate(aggregation_pipeline)))

    parts_df = pd.DataFrame(list(db.part.find(
        {'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS'}},
        {
            'P_PARTKEY': 1,
            'P_MFGR': 1,
            'P_SIZE': 1
        }
    )))

    return suppliers_df, parts_df

# Function to connect to Redis and get partsupp data
def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    partsupp_df = pd.read_json(r.get('partsupp'), orient='records')
    return partsupp_df

# Get data from MongoDB and Redis
suppliers_df, parts_df = get_mongo_data()
partsupp_df = get_redis_data()

# Merge dataframes based on suppliers and parts data
result_df = pd.merge(suppliers_df, parts_df, left_on='S_SUPPKEY', right_on='P_PARTKEY', how='inner')
result_df = pd.merge(result_df, partsupp_df, left_on=['S_SUPPKEY', 'P_PARTKEY'], right_on=['PS_SUPPKEY', 'PS_PARTKEY'], how='inner')

# Filter by minimum PS_SUPPLYCOST within each 'EUROPE' region
result_df = result_df.loc[result_df.groupby('S_SUPPKEY')['PS_SUPPLYCOST'].idxmin()]

# Order results
result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Write output to CSV
result_df.to_csv('query_output.csv', index=False)
