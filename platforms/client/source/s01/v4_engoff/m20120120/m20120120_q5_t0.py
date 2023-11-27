# -*- coding: utf-8 -*-
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
query_mysql = '''
    SELECT R_REGIONKEY, L_EXTENDEDPRICE, L_DISCOUNT
    FROM lineitem
    JOIN region ON R_NAME = 'ASIA'
    WHERE L_SHIPDATE BETWEEN '1990-01-01' AND '1995-01-01';
'''
lineitem_df = pd.read_sql(query_mysql, mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_df = pd.DataFrame(list(mongo_db.customer.find({'C_NATIONKEY': {'$in': lineitem_df['R_REGIONKEY'].unique().tolist()}})))

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))
orders_df = pd.read_json(redis_conn.get('orders'))

# Filtering data
asia_nations = nation_df[nation_df['N_REGIONKEY'].isin(lineitem_df['R_REGIONKEY'])]
asia_customers = customer_df[customer_df['C_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]
asia_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(asia_nations['N_NATIONKEY'])]
asia_orders = orders_df[orders_df['O_CUSTKEY'].isin(asia_customers['C_CUSTKEY'])]

# Merging data and calculating revenue
merge1 = pd.merge(asia_orders, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merge2 = pd.merge(merge1, asia_nations, left_on='O_CUSTKEY', right_on='N_NATIONKEY')
merge2['REVENUE'] = merge2['L_EXTENDEDPRICE'] * (1 - merge2['L_DISCOUNT'])
final_df = merge2.groupby('N_NAME')['REVENUE'].sum().reset_index()
final_df = final_df.sort_values(by='REVENUE', ascending=False)

# Writing results to file
final_df.to_csv('query_output.csv', index=False)
