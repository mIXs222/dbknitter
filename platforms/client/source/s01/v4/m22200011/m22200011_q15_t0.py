import pymysql
import pymongo
import pandas as pd
from datetime import datetime, timedelta

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get suppliers from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM supplier")
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Get lineitem from MongoDB
lineitem_collection = mongo_db['lineitem']
end_date = datetime(1996, 1, 1) + timedelta(months=3)
lineitems = pd.DataFrame(list(lineitem_collection.find(
    {'L_SHIPDATE': {'$gte': datetime(1996, 1, 1), '$lt': end_date}}
)))

# Calculate revenue0
lineitems['TOTAL_REVENUE'] = lineitems['L_EXTENDEDPRICE'] * (1 - lineitems['L_DISCOUNT'])
revenue0 = lineitems.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()
revenue0.columns = ['SUPPLIER_NO', 'TOTAL_REVENUE']
max_revenue = revenue0['TOTAL_REVENUE'].max()
revenue0 = revenue0[revenue0['TOTAL_REVENUE'] == max_revenue]

# Merge the results
result = suppliers.merge(revenue0, left_on='S_SUPPKEY', right_on='SUPPLIER_NO')

# Selecting the required columns for output
result = result[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Saving the result to a CSV file
result.to_csv('query_output.csv', index=False)

# Closing the connections
mysql_conn.close()
mongo_client.close()
