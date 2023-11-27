# import libraries
import pandas as pd
import pymysql
import pymongo
import direct_redis
from datetime import datetime

# Function to execute a query on MySQL
def execute_mysql_query(query, connection_details):
    try:
        connection = pymysql.connect(
            host=connection_details['hostname'],
            user=connection_details['username'],
            password=connection_details['password'],
            db=connection_details['database'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.Cursor
        )
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(list(result))
            
            if not df.empty:
                df.columns = [desc[0] for desc in cursor.description]
            return df
    finally:
        connection.close()

# Function to execute a query on MongoDB
def execute_mongodb_query(collection_name, query, connection_details):
    client = pymongo.MongoClient(
        host=connection_details['hostname'],
        port=connection_details['port']
    )
    db = client[connection_details['database']]
    collection = db[collection_name]
    result = collection.find(query)
    df = pd.DataFrame(list(result))
    return df

# Function to get data from Redis
def execute_redis_query(table_name, connection_details):
    client = direct_redis.DirectRedis(
        host=connection_details['hostname'],
        port=connection_details['port'],
        db=connection_details['database']
    )
    df_json_str = client.get(table_name).decode('utf-8')
    df = pd.read_json(df_json_str)
    return df

# Define connection details
mysql_conn_details = {
    "database": "tpch",
    "username": "root",
    "password": "my-secret-pw",
    "hostname": "mysql"
}

mongodb_conn_details = {
    "database": "tpch",
    "port": 27017,
    "hostname": "mongodb"
}

redis_conn_details = {
    "database": 0,
    "port": 6379,
    "hostname": "redis"
}

# Create MySQL query that involves only lineitem and supplier tables
mysql_query = """
SELECT
    lineitem.L_ORDERKEY,
    lineitem.L_EXTENDEDPRICE,
    lineitem.L_DISCOUNT,
    lineitem.L_SHIPDATE,
    lineitem.L_SUPPKEY,
    supplier.S_SUPPKEY,
    supplier.S_NATIONKEY
FROM
    lineitem
JOIN
    supplier ON lineitem.L_SUPPKEY = supplier.S_SUPPKEY
WHERE
    lineitem.L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
"""

# MongoDB query which only has customer table
mongodb_query = {"C_NATIONKEY": {"$exists": True}}

# Redis key names for tables
redis_keys = ['nation', 'orders']

# Execute queries
lineitem_supplier_df = execute_mysql_query(mysql_query, mysql_conn_details)
customer_df = execute_mongodb_query("customer", mongodb_query, mongodb_conn_details)
nation_df = execute_redis_query(redis_keys[0], redis_conn_details)
orders_df = execute_redis_query(redis_keys[1], redis_conn_details)

# Convert shipdate column to datetime
lineitem_supplier_df['L_SHIPDATE'] = pd.to_datetime(lineitem_supplier_df['L_SHIPDATE'])

# Filter lineitem_supplier by the relevant supplier nations
relevant_nations = nation_df[nation_df['N_NAME'].isin(['JAPAN', 'INDIA'])]
lineitem_supplier_df = lineitem_supplier_df[lineitem_supplier_df['S_NATIONKEY'].isin(relevant_nations['N_NATIONKEY'])]

# Join the orders and filter by orderkey
orders_df.set_index('O_ORDERKEY', inplace=True)
lineitem_supplier_df.set_index('L_ORDERKEY', inplace=True)
lineitem_orders_df = lineitem_supplier_df.join(orders_df, how='inner')

# Join the customers and filter by custkey
customer_df.set_index('C_CUSTKEY', inplace=True)
final_df = lineitem_orders_df.join(customer_df, on='O_CUSTKEY', how='inner')

# Apply the filtering conditions for nationkeys and calculate volume
final_df = final_df.merge(
    relevant_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY'
).rename(columns={'N_NAME': 'SUPP_NATION'})

final_df = final_df.merge(
    relevant_nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY'
).rename(columns={'N_NAME': 'CUST_NATION'})

# Filter based on the nation names for supplier and customer being INDIA and JAPAN
final_df = final_df[
    ((final_df['SUPP_NATION'] == 'JAPAN') & (final_df['CUST_NATION'] == 'INDIA')) |
    ((final_df['SUPP_NATION'] == 'INDIA') & (final_df['CUST_NATION'] == 'JAPAN'))
]

# Calculate revenue
final_df['VOLUME'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])
final_df['L_YEAR'] = final_df['L_SHIPDATE'].dt.year

# Group by the necessary columns and calculate revenue
result_df = final_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR']).agg({'VOLUME': 'sum'}).reset_index()

# Order the result
result_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], inplace=True)

# Rename the columns to match the expected output
result_df.rename(columns={'VOLUME': 'REVENUE'}, inplace=True)

# Save the output to a CSV file
result_df.to_csv('query_output.csv', index=False)
