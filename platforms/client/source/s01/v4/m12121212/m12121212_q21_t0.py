import pymongo
import pandas as pd
import direct_redis

# Set up MongoDB connection
def get_mongo_client(hostname='mongodb', port=27017, db_name='tpch'):
    client = pymongo.MongoClient(f'mongodb://{hostname}:{port}/')
    db = client[db_name]
    return db

# Fetch data from MongoDB collections
def fetch_mongo_data(db):
    nations = pd.DataFrame(list(db.nation.find({"N_NAME": "SAUDI ARABIA"}, {"_id": 0})))
    orders = pd.DataFrame(list(db.orders.find({"O_ORDERSTATUS": "F"}, {"_id": 0})))
    return nations, orders

# Set up Redis connection
def get_redis_client(hostname='redis', port=6379, db_name=0):
    return direct_redis.DirectRedis(host=hostname, port=port, db=db_name)

# Fetch data from Redis
def fetch_redis_data(r):
    supplier = pd.read_json(r.get('supplier'))
    lineitem = pd.read_json(r.get('lineitem'))
    return supplier, lineitem

# Perform the complex query using Pandas
def complex_query(nations, orders, supplier, lineitem):
    # Merge Redis and MongoDB data
    df_merged = lineitem.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY').merge(
        supplier, left_on='L_SUPPKEY', right_on='S_SUPPKEY').merge(
        nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Filter out the data according to the SQL query conditions
    df_filtered = df_merged[(df_merged['L_RECEIPTDATE'] > df_merged['L_COMMITDATE'])]

    # Perform the EXISTS subquery logic using Pandas
    df_exists_subquery = lineitem[lineitem['L_ORDERKEY'].isin(df_filtered['L_ORDERKEY']) & ~lineitem['L_SUPPKEY'].isin(df_filtered['L_SUPPKEY'])]
    
    # Perform the NOT EXISTS subquery logic using Pandas
    df_not_exists_subquery = lineitem[lineitem['L_ORDERKEY'].isin(df_filtered['L_ORDERKEY']) & ~lineitem['L_SUPPKEY'].isin(df_filtered['L_SUPPKEY']) & (lineitem['L_RECEIPTDATE'] > lineitem['L_COMMITDATE'])]

    # Processing the EXISTS condition
    df_filtered = df_filtered[df_filtered['L_ORDERKEY'].isin(df_exists_subquery['L_ORDERKEY'])]

    # Processing the NOT EXISTS condition
    df_filtered = df_filtered[~df_filtered['L_ORDERKEY'].isin(df_not_exists_subquery['L_ORDERKEY'])]
    
    # Perform GROUP BY and ORDER BY operation in Pandas
    df_result = df_filtered.groupby('S_NAME').size().reset_index(name='NUMWAIT').sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

    # Write the result to CSV
    df_result.to_csv('query_output.csv', index=False)

def main():
    # Connect to MongoDB
    mongo_db = get_mongo_client()
    nations, orders = fetch_mongo_data(mongo_db)

    # Connect to Redis
    redis_client = get_redis_client()
    supplier, lineitem = fetch_redis_data(redis_client)

    # Execute query
    complex_query(nations, orders, supplier, lineitem)

if __name__ == '__main__':
    main()
