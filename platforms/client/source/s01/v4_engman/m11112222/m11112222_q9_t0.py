# python code to execute the query (save as execute_query.py)

from pymongo import MongoClient
import pandas as pd
import redis as rd


def get_mongodb_data():
    client = MongoClient("mongodb", 27017)
    db = client.tpch
    
    nation = pd.DataFrame(list(db.nation.find({}, {"_id": 0})))
    supplier = pd.DataFrame(list(db.supplier.find({}, {"_id": 0})))
    
    client.close()
    return nation, supplier


def get_redis_data():
    redis_client = rd.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
    partsupp = pd.read_json(redis_client.get('partsupp'))
    lineitem = pd.read_json(redis_client.get('lineitem'))
    
    return partsupp, lineitem


def main():
    # Connect to MongoDB and get data
    nation, supplier = get_mongodb_data()

    # Connect to Redis and get data
    partsupp, lineitem = get_redis_data()

    # Now we need to merge and compute the results according to the specified query
    # Step 1: Merge dataframes to have all required data in one dataframe
    merged_df = (
        lineitem
        .merge(partsupp, how='left', on=['PS_PARTKEY', 'PS_SUPPKEY'])
        .merge(supplier, how='left', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
        .merge(nation, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    )

    # Step 2: Calculate the profit as per the given formula
    merged_df['PROFIT'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])
    
    # Step 3: Group by N_NAME and L_SHIPDATE (extracted year), and then calculate total profit
    merged_df['YEAR'] = pd.DatetimeIndex(merged_df['L_SHIPDATE']).year
    result = (
        merged_df
        .groupby(['N_NAME', 'YEAR'])
        .agg({'PROFIT': 'sum'})
        .reset_index()
    )

    # Step 4: Sort results as per requirements
    result = result.sort_values(['N_NAME', 'YEAR'], ascending=[True, False])
    
    # Write result to CSV
    result.to_csv('query_output.csv', index=False)


if __name__ == "__main__":
    main()
