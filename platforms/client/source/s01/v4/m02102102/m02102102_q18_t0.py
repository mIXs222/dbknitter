# Python code: query_exec.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

def execute_query():

    # Connect to MySQL
    mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    # Get orders data from MySQL
    orders_query = "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE FROM orders"
    orders_df = pd.read_sql(orders_query, mysql_conn)
    mysql_conn.close()

    # Connect to MongoDB
    mongo_client = pymongo.MongoClient('mongodb', 27017)
    mongo_db = mongo_client['tpch']
    # Get customer data from MongoDB
    customer_data = list(mongo_db.customer.find({}, {'_id': 0, 'C_NAME': 1, 'C_CUSTKEY': 1}))
    customer_df = pd.DataFrame(customer_data)

    # Connect to Redis
    redis_conn = DirectRedis(host='redis', port=6379, db=0)
    # Get lineitem data from Redis
    lineitem_df = pd.read_msgpack(redis_conn.get('lineitem'))

    # Filter lineitem data with the SUM of L_QUANTITY > 300
    lineitem_filtered_df = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300)

    # Merge dataframes
    order_lineitem_df = pd.merge(orders_df, lineitem_filtered_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    result_df = pd.merge(customer_df, order_lineitem_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

    # Group by and sum L_QUANTITY
    final_df = result_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({
        'L_QUANTITY': 'sum'
    }).reset_index()

    # Sort by O_TOTALPRICE and O_ORDERDATE
    final_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

    # Write to CSV file
    final_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
