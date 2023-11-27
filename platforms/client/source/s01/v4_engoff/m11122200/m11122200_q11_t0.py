import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Connection to MongoDB
def mongodb_connection(hostname, port, database_name):
    client = pymongo.MongoClient(f"mongodb://{hostname}:{port}/")
    db = client[database_name]
    return db

# Connection to Redis
def redis_connection(hostname, port, database_name):
    return DirectRedis(host=hostname, port=port, db=database_name)

# Main execution method
def main():
    # MongoDB connection
    mongodb_db = mongodb_connection(hostname='mongodb', port=27017, database_name='tpch')
    nation_collection = mongodb_db['nation']
    
    # Redis connection
    redis_db = redis_connection(hostname='redis', port=6379, database_name=0)
    
    # Querying data from MongoDB and Redis
    nations_df = pd.DataFrame(list(nation_collection.find({'N_NAME': 'GERMANY'}, {'N_NATIONKEY': 1, 'N_NAME': 1})))
    supplier_df = pd.read_json(redis_db.get('supplier'))
    partsupp_df = pd.read_json(redis_db.get('partsupp'))
    
    # Filter German suppliers
    german_supplier_keys = nations_df['N_NATIONKEY'].unique()
    german_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(german_supplier_keys)]
    
    # Merge with partsupp
    german_parts_value = pd.merge(german_suppliers[['S_SUPPKEY']], partsupp_df, how='inner', left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
    german_parts_value['VALUE'] = german_parts_value['PS_AVAILQTY'] * german_parts_value['PS_SUPPLYCOST']
    
    # Calculate significant parts and sort them
    total_value = german_parts_value['VALUE'].sum()
    significant_parts = german_parts_value[german_parts_value['VALUE'] > (total_value * 0.0001)]
    significant_parts_sorted = significant_parts.sort_values(by='VALUE', ascending=False)[['PS_PARTKEY', 'VALUE']]
    
    # Write the result to CSV
    significant_parts_sorted.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
