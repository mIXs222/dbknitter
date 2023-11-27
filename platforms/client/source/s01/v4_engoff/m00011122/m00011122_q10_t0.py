import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis
import datetime

# Function to connect to MySQL and fetch nation and order data.
def fetch_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    try:
        with connection.cursor() as cursor:
            nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation;"
            cursor.execute(nation_query)
            nations = cursor.fetchall()
            return pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])
    finally:
        connection.close()

# Function to connect to MongoDB and fetch customer data.
def fetch_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    customer_data = list(db.customer.find({}, {
        '_id': False,
        'C_CUSTKEY': True,
        'C_NAME': True,
        'C_ADDRESS': True,
        'C_NATIONKEY': True,
        'C_PHONE': True,
        'C_ACCTBAL': True,
        'C_COMMENT': True
    }))
    return pd.DataFrame(customer_data)

# Function to connect to Redis and fetch order and lineitem data.
def fetch_redis_data():
    r = DirectRedis(host='redis', port=6379, db=0)
    
    orders_df = pd.read_json(r.get('orders'), orient='records')
    lineitem_df = pd.read_json(r.get('lineitem'), orient='records')

    # Filter order dates.
    date_start = datetime.datetime(1993, 10, 1)
    date_end = datetime.datetime(1994, 1, 1)
    orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
    filtered_orders_df = orders_df[(orders_df['O_ORDERDATE'] >= date_start) & (orders_df['O_ORDERDATE'] <= date_end)]

    # Merge filtered orders with lineitem and calculate lost revenue.
    merged_lineitem_orders = pd.merge(filtered_orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')
    merged_lineitem_orders['REVENUE_LOST'] = merged_lineitem_orders['L_EXTENDEDPRICE'] * (1 - merged_lineitem_orders['L_DISCOUNT'])
    
    return merged_lineitem_orders.groupby('O_CUSTKEY')['REVENUE_LOST'].sum().reset_index()

# Fetch data from different databases.
nation_data = fetch_mysql_data()
customer_data = fetch_mongodb_data()
redis_data = fetch_redis_data()

# Merge customer with nation data.
customer_nation_df = pd.merge(customer_data, nation_data, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')

# Merge customer_nation with redis data.
reporting_df = pd.merge(customer_nation_df, redis_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='inner')

# Reorder columns and sort by revenue lost, customer key, name and balance.
output_df = reporting_df[['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'REVENUE_LOST']]
output_df = output_df.sort_values(by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True])

# Write result to file.
output_df.to_csv('query_output.csv', index=False)
