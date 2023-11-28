import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to get data from MySQL
def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            query = "SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'SAUDI ARABIA';"
            cursor.execute(query)
            nations = cursor.fetchall()
    finally:
        connection.close()
    return nations

# Function to get data from MongoDB
def get_mongo_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client.tpch
    suppliers = list(db.supplier.find({"S_NATIONKEY": {"$in": [n[0] for n in get_mysql_data()]}}))
    return suppliers

# Function to get data from Redis and format it as DataFrame
def get_redis_data():
    r = DirectRedis(host='redis', port=6379, db=0)
    orders_df = pd.DataFrame(r.get('orders'))
    lineitem_df = pd.DataFrame(r.get('lineitem'))
    return orders_df, lineitem_df

# Combine data from different databases
def combine_data():
    suppliers_data = get_mongo_data()
    orders_df, lineitem_df = get_redis_data()

    # Search for suppliers in the "orders" and "lineitem" dataframes
    orders_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']
    lineitem_df = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

    # Create a supplier dataframe
    suppliers_df = pd.DataFrame(suppliers_data)

    # Merge the dataframes to get the final result
    result = (
        suppliers_df.merge(lineitem_df, how='inner', left_on='S_SUPPKEY', right_on='L_SUPPKEY')
        .merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
        .groupby('S_NAME')
        .agg(NUMWAIT=('L_ORDERKEY', 'count'))
        .reset_index()
        .sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])
    )

    return result

# Save the result to CSV
result_data = combine_data()
result_data.to_csv('query_output.csv', index=False)
