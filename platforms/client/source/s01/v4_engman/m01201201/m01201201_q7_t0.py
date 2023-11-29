import pymysql
import pymongo
import pandas as pd
import direct_redis
from datetime import datetime

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch customer data from Redis
customer_data = redis_conn.get('customer')
customers_df = pd.read_json(customer_data)

# Fetch nation and supplier data from MySQL
query_nation = "SELECT * FROM nation WHERE N_NAME IN ('INDIA', 'JAPAN');"
query_supplier = "SELECT * FROM supplier;"
mysql_cursor.execute(query_nation)
nations_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
mysql_cursor.execute(query_supplier)
suppliers_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Fetch lineitem data from MongoDB
query_lineitem = {
    'L_SHIPDATE': {
        '$gte': datetime(1995, 1, 1),
        '$lte': datetime(1996, 12, 31)
    }
}
lineitems_cursor = lineitem_collection.find(query_lineitem)
lineitems_df = pd.DataFrame(list(lineitems_cursor))

# Data preparation and transformation
joined_df = lineitems_df.merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
joined_df = joined_df.merge(nations_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
joined_df = joined_df.merge(customers_df, left_on='L_ORDERKEY', right_on='C_CUSTKEY')

# Filter for the relevant nations and calculate the revenue
joined_df = joined_df[(joined_df['N_NAME_x'].isin(['INDIA', 'JAPAN'])) & (joined_df['N_NAME_y'].isin(['INDIA', 'JAPAN'])) & (joined_df['N_NAME_x'] != joined_df['N_NAME_y'])]
joined_df['L_YEAR'] = pd.to_datetime(joined_df['L_SHIPDATE']).dt.year
joined_df['REVENUE'] = joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])

# Group by the results
final_df = joined_df.groupby(['N_NAME_y', 'L_YEAR', 'N_NAME_x'])['REVENUE'].sum().reset_index()
final_df.columns = ['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']

# Order the results
final_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], ascending=True, inplace=True)

# Save to CSV
final_df.to_csv('query_output.csv', index=False)
