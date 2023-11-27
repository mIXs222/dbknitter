import pymongo
import pandas as pd
import direct_redis

def get_mongodb_data():
    client = pymongo.MongoClient('mongodb://mongodb:27017/')
    db = client['tpch']
    parts = pd.DataFrame(list(db['part'].find({"P_SIZE": 15, "P_TYPE": {'$regex': "BRASS$"}})))
    partsupp = pd.DataFrame(list(db['partsupp'].find()))
    nation = pd.DataFrame(list(db['nation'].find()))
    return parts, partsupp, nation

def get_redis_data(redis_connection):
    region_df = pd.read_json(redis_connection.get('region'))
    supplier_df = pd.read_json(redis_connection.get('supplier'))
    return region_df, supplier_df

def query_data(parts, partsupp, supplier_df, nation, region_df):
    # Filtering the region
    europe_region = region_df[region_df['R_NAME'] == 'EUROPE']
    # Joining dataframes
    combined = parts.merge(partsupp, left_on='P_PARTKEY', right_on='PS_PARTKEY')
    combined = combined.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    combined = combined.merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    combined = combined.merge(europe_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
    # Sub-query to find the minimum PS_SUPPLYCOST
    min_cost = combined[combined['R_NAME'] == 'EUROPE']['PS_SUPPLYCOST'].min()
    result = combined[combined['PS_SUPPLYCOST'] == min_cost]
    # Ordering the final result
    final_result = result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])
    # Selecting specific columns
    final_result = final_result[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]
    return final_result

def main():
    # Fetch data from MongoDB
    parts, partsupp, nation = get_mongodb_data()
    # Connect to Redis using direct_redis
    redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    # Fetch data from Redis
    region_df, supplier_df = get_redis_data(redis_connection)
    # Running the query
    query_result = query_data(parts, partsupp, supplier_df, nation, region_df)
    # Write result to CSV
    query_result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
