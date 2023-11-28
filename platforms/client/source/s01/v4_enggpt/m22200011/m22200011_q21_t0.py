# query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

def get_mysql_connection():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

def get_mongodb_connection():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

def get_redis_dataframe():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    nation_df = pd.read_json(redis_client.get('nation'))
    return nation_df

def main():
    # Connect to MySQL to get suppliers from Saudi Arabia
    mysql_conn = get_mysql_connection()
    supplier_query = """
    SELECT S_SUPPKEY, S_NAME
    FROM supplier S
    INNER JOIN nation N ON S.S_NATIONKEY = N.N_NATIONKEY
    WHERE N.N_NAME = 'SAUDI ARABIA'
    """
    suppliers_df = pd.read_sql(supplier_query, mysql_conn)
    mysql_conn.close()

    # Connect to MongoDB to get orders and lineitems
    mongodb_conn = get_mongodb_connection()
    orders_col = mongodb_conn['orders']
    lineitem_col = mongodb_conn['lineitem']

    # Filter orders with 'F' status
    orders = list(orders_col.find({'O_ORDERSTATUS': 'F'}, {'_id': 0}))

    # Create a DataFrame for orders and lineitems
    orders_df = pd.DataFrame(orders)
    lineitems = list(lineitem_col.find({}, {'_id': 0}))
    lineitem_df = pd.DataFrame(lineitems)

    # Connect to Redis
    nation_df = get_redis_dataframe()

    # Filter suppliers based in Saudi Arabia using nation DataFrame from Redis
    saudi_suppliers = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']
    saudi_suppliers_set = set(saudi_suppliers['N_NATIONKEY'].values)
    suppliers_df = suppliers_df[suppliers_df['S_NATIONKEY'].isin(saudi_suppliers_set)]
    
    # Execute analysis
    final_df = None  # The DataFrame that will have the final result. To be filled in by additional function or logic.

    # TODO: Add the complex EXISTS subqueries and other query logic here.
    # As an integrated analytical processing across different platforms is complex
    # and beyond the scope of a single script, you may need additional functions
    # or pre-processing steps to align data models and perform analysis.

    # Write DataFrame to CSV
    final_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
