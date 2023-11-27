import pymysql
import pymongo
from datetime import datetime
import pandas as pd
import direct_redis

# Function to connect to MySQL and retrieve the relevant data
def get_mysql_data():
    connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT
                C_CUSTKEY, C_NATIONKEY
            FROM
                customer
            """
            cursor.execute(query)
            customer_data = cursor.fetchall()

            query = """
            SELECT
                S_SUPPKEY, S_NATIONKEY
            FROM
                supplier
            """
            cursor.execute(query)
            supplier_data = cursor.fetchall()

        customer_df = pd.DataFrame(list(customer_data), columns=['C_CUSTKEY', 'C_NATIONKEY'])
        supplier_df = pd.DataFrame(list(supplier_data), columns=['S_SUPPKEY', 'S_NATIONKEY'])
        return customer_df, supplier_df
    finally:
        connection.close()

# Function to connect to MongoDB and retrieve the relevant data
def get_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    orders_collection = db['orders']
    nation_collection = db['nation']

    orders_df = pd.DataFrame(list(orders_collection.find({})))
    nation_df = pd.DataFrame(list(nation_collection.find({})))

    client.close()
    return orders_df, nation_df

# Function to retrieve data from Redis and convert it to a DataFrame
def get_redis_data():
    redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    lineitem_df = pd.read_json(redis_client.get('lineitem'))
    return lineitem_df

# Connect to each database and retrieve their respective tables
customer_df, supplier_df = get_mysql_data()
orders_df, nation_df = get_mongodb_data()
lineitem_df = get_redis_data()

# Merge the dataframes accordingly
merged_df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

merged_df = merged_df.merge(nation_df.rename(columns={'N_NAME': 'SUPP_NATION', 'N_NATIONKEY': 'S_NATIONKEY'}), on='S_NATIONKEY')
merged_df = merged_df.merge(nation_df.rename(columns={'N_NAME': 'CUST_NATION', 'N_NATIONKEY': 'C_NATIONKEY'}), on='C_NATIONKEY')

# Filter based on given conditions
filtered_df = merged_df[
    ((merged_df['SUPP_NATION'] == 'JAPAN') & (merged_df['CUST_NATION'] == 'INDIA')) |
    ((merged_df['SUPP_NATION'] == 'INDIA') & (merged_df['CUST_NATION'] == 'JAPAN')) &
    (merged_df['L_SHIPDATE'] >= datetime(1995, 1, 1)) &
    (merged_df['L_SHIPDATE'] <= datetime(1996, 12, 31))
]

# Create L_YEAR column and calculate VOLUME
filtered_df['L_YEAR'] = filtered_df['L_SHIPDATE'].dt.year
filtered_df['VOLUME'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by the necessary columns and sort
result_df = filtered_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])['VOLUME'].sum().reset_index()
result_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Save the results to query_output.csv
result_df.to_csv('query_output.csv', index=False)
