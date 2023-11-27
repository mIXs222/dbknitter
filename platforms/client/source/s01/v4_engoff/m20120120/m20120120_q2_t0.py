# query.py
import pymysql
import pymongo
import pandas as pd
import direct_redis
from pymongo import MongoClient

# Function to execute query on MySQL
def query_mysql():
    mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    cursor = mysql_conn.cursor()
    cursor.execute("""
        SELECT r.R_REGIONKEY
        FROM region r
        WHERE r.R_NAME = 'EUROPE'
    """)
    europe_region_key = cursor.fetchone()[0]
    cursor.execute("""
        SELECT ps.PS_PARTKEY, PS_SUPPKEY, PS_SUPPLYCOST
        FROM partsupp ps
    """)
    partsupp_df = pd.DataFrame(cursor.fetchall(), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST'])
    mysql_conn.close()
    return europe_region_key, partsupp_df

# Function to execute query on MongoDB
def query_mongodb():
    mongo_client = MongoClient('mongodb', 27017)
    mongo_db = mongo_client['tpch']
    part_collection = mongo_db['part']
    part_df = pd.DataFrame(list(part_collection.find({'P_TYPE': 'BRASS', 'P_SIZE': 15},
                                                     {'_id': 0, 'P_PARTKEY': 1, 'P_MFGR': 1})))
    mongo_client.close()
    return part_df

# Function to query Redis database
def query_redis():
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    nation_df = pd.read_json(redis_client.get('nation').decode('utf-8'))
    supplier_df = pd.read_json(redis_client.get('supplier').decode('utf-8'))
    return nation_df, supplier_df

def main():
    # Execute queries on different databases
    europe_region_key, partsupp_df = query_mysql()
    part_df = query_mongodb()
    nation_df, supplier_df = query_redis()

    # Filter nation and supplier details by region key 'EUROPE'
    nation_df = nation_df[nation_df['N_REGIONKEY'] == europe_region_key]
    supplier_df = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df['N_NATIONKEY'])]

    # Merge DataFrames to combine all the necessary information
    combined_df = partsupp_df.merge(part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
    combined_df = combined_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    combined_df = combined_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Sort by minimum cost, S_ACCTBAL, N_NAME, and S_NAME
    sorted_df = combined_df.sort_values(by=['PS_SUPPLYCOST', 'S_ACCTBAL', 'N_NAME', 'S_NAME'], 
                                        ascending=[True, False, True, True])

    # Write result to CSV
    sorted_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
