import pymongo
import pandas as pd
from bson import json_util
import direct_redis

# Function to connect to MongoDB and return the partsupp collection as DataFrame
def get_partsupp_from_mongodb():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    partsupp_data = list(db.partsupp.find({}, {'_id': 0}))
    df_partsupp = pd.json_normalize(partsupp_data)
    client.close()
    return df_partsupp

# Function to connect to Redis and return the nation and supplier tables as DataFrames
def get_nation_supplier_from_redis():
    rd = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    nation_df = pd.read_json(rd.get('nation'), orient='records')
    supplier_df = pd.read_json(rd.get('supplier'), orient='records')
    return nation_df, supplier_df

# Retrieve data
df_partsupp = get_partsupp_from_mongodb()
nation_df, supplier_df = get_nation_supplier_from_redis()

# Prepare DataFrames for merge (rename columns to be consistent with query)
df_partsupp.rename(columns={'PS_PARTKEY': 'PS_PARTKEY', 
                            'PS_SUPPKEY': 'PS_SUPPKEY', 
                            'PS_AVAILQTY': 'PS_AVAILQTY', 
                            'PS_SUPPLYCOST': 'PS_SUPPLYCOST'}, inplace=True)

supplier_df.rename(columns={'S_SUPPKEY': 'PS_SUPPKEY', 
                            'S_NATIONKEY': 'S_NATIONKEY'}, inplace=True)

nation_df.rename(columns={'N_NATIONKEY': 'S_NATIONKEY', 'N_NAME': 'N_NAME'}, inplace=True)

# Filter nation for 'GERMANY'
nation_df = nation_df[nation_df['N_NAME'] == 'GERMANY']

# Merging the DataFrames
df_merged = df_partsupp.merge(supplier_df, on='PS_SUPPKEY').merge(nation_df, on='S_NATIONKEY')

# Perform the computation for the main SELECT
df_result = df_merged.groupby('PS_PARTKEY').apply(lambda x: pd.Series({
    'VALUE': (x['PS_SUPPLYCOST'] * x['PS_AVAILQTY']).sum()
}))

# Filter out the noisy data as per HAVING clause in SQL query
subq_value = df_result['VALUE'].sum() * 0.0001000000
df_result = df_result[df_result['VALUE'] > subq_value]

# Sort the results
df_result.sort_values('VALUE', ascending=False, inplace=True)

# Write results to CSV
df_result.to_csv('query_output.csv')
