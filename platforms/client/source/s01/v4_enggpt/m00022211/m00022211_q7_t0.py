# Python code to execute the query (query.py)

import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Function to convert to datetime
def convert_to_datetime(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d')

# Connection to MySQL (for nation table)
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM nation WHERE N_NAME IN ('JAPAN', 'INDIA')")
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])
mysql_conn.close()

# Connection to MongoDB (for orders and lineitem tables)
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
orders_coll = mongo_db['orders']
lineitem_coll = mongo_db['lineitem']

# Get orders and lineitem within the specified date range
orders_df = pd.DataFrame(list(orders_coll.find({})))
orders_df = orders_df[orders_df['O_ORDERDATE'].apply(convert_to_datetime).between(datetime(1995, 1, 1), datetime(1996, 12, 31))]

lineitem_df = pd.DataFrame(list(lineitem_coll.find({})))
lineitem_df['revenue'] = lineitem_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)

# Connection to Redis (for supplier and customer tables)
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_json(redis_client.get('supplier'))
customer_df = pd.read_json(redis_client.get('customer'))

# Merge the dataframes to prepare for the report
merged_df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_SUPPLIER', '_CUSTOMER'))

# Filter specific relations between Japan and India
report_df = merged_df[
    (merged_df['N_NAME_SUPPLIER'].isin(['JAPAN', 'INDIA'])) &
    (merged_df['N_NAME_CUSTOMER'].isin(['JAPAN', 'INDIA'])) &
    (merged_df['N_NAME_SUPPLIER'] != merged_df['N_NAME_CUSTOMER'])]

# Generating the report
report_df['year'] = report_df['L_SHIPDATE'].apply(lambda date: datetime.strptime(date, '%Y-%m-%d').year)
final_report = report_df.groupby(['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'year']).agg({'revenue': 'sum'}).reset_index()

# Export to CSV
final_report.sort_values(by=['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'year']).to_csv('query_output.csv', index=False)
