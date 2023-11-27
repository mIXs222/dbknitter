import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_cur = mysql_conn.cursor()
mysql_cur.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
nation_data = pd.DataFrame(mysql_cur.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])
mysql_cur.close()
mysql_conn.close()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_col = mongodb['orders']
lineitem_col = mongodb['lineitem']
orders_data = pd.DataFrame(list(orders_col.find({
    "O_ORDERDATE": {"$gte": "1993-10-01", "$lt": "1994-01-01"}
})))
lineitem_data = pd.DataFrame(list(lineitem_col.find()))

# Join orders and lineitem DataFrames
orders_lineitem_data = orders_data.merge(lineitem_data, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Compute lost revenue and filter by line status 'R' for returned
orders_lineitem_data['LOST_REVENUE'] = orders_lineitem_data['L_EXTENDEDPRICE'] * (1 - orders_lineitem_data['L_DISCOUNT'])
returned_items = orders_lineitem_data[orders_lineitem_data['L_RETURNFLAG'] == 'R']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)
customer_str = redis_conn.get('customer')
customer_df = pd.read_json(customer_str)

# Merge customer information with the lost revenue data
result = customer_df.merge(returned_items, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = result.merge(nation_data, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Select required columns and sort the data as per the requirement
output_columns = [
    'C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'LOST_REVENUE'
]
final_result = result[output_columns]
final_result['LOST_REVENUE'] = final_result['LOST_REVENUE'].sum(axis=1)
final_result.sort_values(by=['LOST_REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True], inplace=True)

# Write the final result to CSV
final_result.to_csv('query_output.csv', index=False)
