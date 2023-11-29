# suppliers_who_kept_orders_waiting.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
)

# Connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load supplier data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT S_SUPPKEY, S_NAME
        FROM supplier
    """)
    suppliers_df = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NAME'])

# Load nation data from MongoDB
nation_col = mongo_db['nation']
nation_df = pd.DataFrame(list(nation_col.find({'N_NAME': 'SAUDI ARABIA'})))

# Load lineitem data from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))
lineitem_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'F']

# Load orders data from Redis
orders_df = pd.read_json(redis_client.get('orders'))
multi_supplier_orders = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_SUPPKEY'].nunique() > 1)

# Merge tables to identify suppliers with awaited lineitems
multi_supplier_orders['L_RECEIPTDATE'] > multi_supplier_orders['L_COMMITDATE']
orders_waiting = orders_df[orders_df['O_ORDERSTATUS'] == 'F']
waiting_lineitems = multi_supplier_orders[multi_supplier_orders['L_ORDERKEY'].isin(orders_waiting['O_ORDERKEY'])]

supplier_waiting_counts = waiting_lineitems.groupby('L_SUPPKEY').size().reset_index(name='NUMWAIT')
supplier_waiting_counts = supplier_waiting_counts.merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')[['NUMWAIT', 'S_NAME']]

supplier_waiting_counts.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True], inplace=True)

# Write the result to CSV
supplier_waiting_counts.to_csv('query_output.csv', index=False, header=True)

# Close all connections
mysql_conn.close()
mongo_client.close()
redis_client.close()
