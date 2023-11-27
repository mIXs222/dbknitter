import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve region EUROPE key from mongodb
europe_key = mongodb['region'].find_one({'R_NAME': 'EUROPE'}, {'R_REGIONKEY': 1})['R_REGIONKEY']

# Select suitable nations from mysql
mysql_cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_REGIONKEY = %s", (europe_key,))
nation_info = {row[0]: row[1] for row in mysql_cursor.fetchall()}

# Get parts from Redis
parts_df = pd.read_msgpack(redis_client.get('part'))
brass_parts_df = parts_df[(parts_df['P_TYPE'] == 'BRASS') & (parts_df['P_SIZE'] == 15)]

# Get partsupp data from mongodb
pipeline = [
    {'$match': {'PS_PARTKEY': {'$in': brass_parts_df['P_PARTKEY'].tolist()}}},
    {'$lookup': {
        'from': "supplier",
        'localField': "PS_SUPPKEY",
        'foreignField': "S_SUPPKEY",
        'as': "supplier_info"
    }},
    {'$unwind': "$supplier_info"},
    {'$match': {"supplier_info.S_NATIONKEY": {'$in': list(nation_info.keys())}}},
    {'$project': {
        '_id': 0,
        'PS_SUPPLYCOST': 1,
        'supplier_info.S_ACCTBAL': 1,
        'supplier_info.S_NAME': 1,
        'supplier_info.S_NATIONKEY': 1,
        'PS_PARTKEY': 1,
        'supplier_info.S_ADDRESS': 1,
        'supplier_info.S_PHONE': 1,
        'supplier_info.S_COMMENT': 1
    }}
]
partsupp_data = mongodb['partsupp'].aggregate(pipeline)
partsupp_df = pd.DataFrame(list(partsupp_data))

# If the DataFrame is empty, we will skip the merging process.
if not partsupp_df.empty:
    # Merge brass_parts_df with partsupp_df on part key.
    merged_df = pd.merge(brass_parts_df, partsupp_df, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

    # Generate output result according to query's pattern.
    output_df = merged_df.sort_values(by=['PS_SUPPLYCOST', 'S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
                                      ascending=[True, False, True, True, True])

    # Adding nation name to output.
    output_df['N_NAME'] = output_df['S_NATIONKEY'].apply(lambda x: nation_info[x])

    # Selecting columns to output.
    output_columns = ['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']
    final_output_df = output_df.loc[:, output_columns]

    # Write to csv file
    final_output_df.to_csv('query_output.csv', index=False)
else:
    print("No suitable data found for the query conditions.")

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
