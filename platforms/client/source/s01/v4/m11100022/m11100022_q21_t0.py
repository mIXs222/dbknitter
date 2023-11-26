# query_executer.py
import pymysql
import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection and query
def fetch_supplier_data():
    mysql_connection = pymysql.connect(host='mysql',
                                       user='root',
                                       password='my-secret-pw',
                                       db='tpch')
    supplier_query = """
    SELECT S_SUPPKEY, S_NAME, S_NATIONKEY
    FROM supplier
    """
    supplier_df = pd.read_sql(supplier_query, mysql_connection)
    mysql_connection.close()
    return supplier_df

# MongoDB connection and query
def fetch_nation_data():
    client = pymongo.MongoClient('mongodb', 27017)
    mongodb = client.tpch
    nation_data = list(mongodb.nation.find({'N_NAME': 'SAUDI ARABIA'}, {'_id': 0, 'N_NATIONKEY': 1}))
    client.close()
    return pd.DataFrame(nation_data)

# Redis connection and data retrieval
def fetch_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    
    # Assuming the Redis store has the data in a format that can be directly converted to Pandas DataFrame
    orders_str = redis_client.get('orders')
    lineitem_str = redis_client.get('lineitem')
    
    # Convert string data to StringIO and then to DataFrame
    orders_df = pd.read_csv(pd.io.common.StringIO(orders_str), sep=",")
    lineitem_df = pd.read_csv(pd.io.common.StringIO(lineitem_str), sep=",")
    
    return orders_df, lineitem_df

# Execution function
def main():
    supplier_df = fetch_supplier_data()
    nation_df = fetch_nation_data()
    
    orders_df, lineitem_df = fetch_redis_data()
    
    merged_df = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
    merged_df = merged_df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    merged_df = merged_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    
    filtered_df = merged_df[
        (merged_df['O_ORDERSTATUS'] == 'F') &
        (merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE'])
    ]
    
    # Applying EXISTS subquery condition
    exists_condition = lineitem_df.loc[lineitem_df['L_ORDERKEY'].isin(filtered_df['L_ORDERKEY']) & 
                                       ~lineitem_df['L_SUPPKEY'].isin(filtered_df['L_SUPPKEY'])]
    
    # Applying NOT EXISTS subquery condition
    not_exists_condition = lineitem_df.loc[
        lineitem_df['L_ORDERKEY'].isin(filtered_df['L_ORDERKEY']) &
        ~lineitem_df['L_SUPPKEY'].isin(filtered_df['L_SUPPKEY']) &
        (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE'])
    ]

    filtered_df = filtered_df[
        filtered_df['L_ORDERKEY'].isin(exists_condition['L_ORDERKEY']) &
        ~filtered_df['L_ORDERKEY'].isin(not_exists_condition['L_ORDERKEY'])
    ]

    final_df = filtered_df.groupby('S_NAME', as_index=False).agg(NUMWAIT=('S_NAME', 'count'))
    final_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)
    
    # Write to CSV
    final_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
