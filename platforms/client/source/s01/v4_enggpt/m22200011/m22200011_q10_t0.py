# python_code.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client.tpch

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Execute the MySQL query to retrieve customer information
sql_query = """
SELECT
    C_CUSTKEY,
    C_NAME,
    C_ACCTBAL,
    C_ADDRESS,
    C_PHONE,
    C_COMMENT
FROM
    customer
"""
customers_df = pd.read_sql(sql_query, mysql_conn)

# Query MongoDB for orders
orders_query = {
    "O_ORDERDATE": {"$gte": "1993-10-01", "$lte": "1993-12-31"},
}
orders_projection = {
    "_id": 0,
    "O_ORDERKEY": 1,
    "O_CUSTKEY": 1,
}
orders_df = pd.DataFrame(list(mongo_db.orders.find(orders_query, orders_projection)))

# Query MongoDB for lineitems with a return flag 'R'
lineitem_query = {
    "L_RETURNFLAG": "R",
}
lineitem_projection = {
    "_id": 0,
    "L_ORDERKEY": 1,
    "L_EXTENDEDPRICE": 1,
    "L_DISCOUNT": 1,
}
lineitems_df = pd.DataFrame(list(mongo_db.lineitem.find(lineitem_query, lineitem_projection)))

# Join orders_df with lineitems_df on 'O_ORDERKEY' and 'L_ORDERKEY'
orders_lineitems_df = pd.merge(orders_df, lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate revenue
orders_lineitems_df['REVENUE'] = orders_lineitems_df['L_EXTENDEDPRICE'] * (1 - orders_lineitems_df['L_DISCOUNT'])

# Group the data by 'O_CUSTKEY' and sum up the revenue
total_revenue = orders_lineitems_df.groupby('O_CUSTKEY')['REVENUE'].sum().reset_index()

# Merge customers_df with total_revenue
result_df = pd.merge(customers_df, total_revenue, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Retrieve nation names from Redis
nation_df = pd.DataFrame(redis_client.get('nation'))

# Merge result_df with nation dataframe
final_df = pd.merge(result_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select and rename columns
final_df = final_df[['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']]
final_df.columns = ['Customer Key', 'Customer Name', 'Total Revenue', 'Account Balance', 'Nation Name', 'Address', 'Phone', 'Comments']

# Sort the data
final_df.sort_values(by=['Total Revenue', 'Customer Key', 'Customer Name'], ascending=[True, True, True], inplace=True)
final_df.sort_values(by='Account Balance', ascending=False, inplace=True)

# Write the results to a CSV file
final_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
