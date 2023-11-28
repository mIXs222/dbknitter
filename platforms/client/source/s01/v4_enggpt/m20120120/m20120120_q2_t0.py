import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from pymongo import MongoClient

def connect_mysql():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )

def connect_mongodb():
    return MongoClient(host='mongodb', port=27017)

def connect_redis():
    return DirectRedis(host='redis', port=6379, db=0)

def get_data():
    mysql_conn = connect_mysql()
    mongodb = connect_mongodb()
    redis_conn = connect_redis()

    # Fetch data from MySQL tpch database
    with mysql_conn:
        partsupp_df = pd.read_sql("SELECT * FROM partsupp", mysql_conn)
        region_df = pd.read_sql("SELECT * FROM region WHERE R_NAME = 'EUROPE'", mysql_conn)
    
    # Fetch data from MongoDB tpch database
    mongodb_tpch = mongodb['tpch']
    part_cursor = mongodb_tpch.part.find({"P_SIZE": 15, "P_TYPE": {"$regex": "BRASS"}})
    part_df = pd.DataFrame(list(part_cursor))

    # Fetch data from Redis database
    nation_df = pd.read_msgpack(redis_conn.get('nation'))
    supplier_df = pd.read_msgpack(redis_conn.get('supplier'))

    # Combine the dataframes
    # Merge only relevant columns from the region and nation tables
    nations_in_region_df = nation_df.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
    suppliers_in_nations_df = supplier_df.merge(nations_in_region_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Merge parts and partsupp on part key
    partsupp_parts_df = partsupp_df.merge(part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')

    combined_df = suppliers_in_nations_df.merge(partsupp_parts_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

    # Filter out suppliers who do not have the minimum supply cost within the EUROPE region
    europe_min_cost_df = combined_df.loc[combined_df.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].idxmin()]

    # Select relevant columns and order the data
    result_df = europe_min_cost_df[[
        'S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT',
        'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'N_NAME'
    ]]

    result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

    # Export the data to a CSV file
    result_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    get_data()
