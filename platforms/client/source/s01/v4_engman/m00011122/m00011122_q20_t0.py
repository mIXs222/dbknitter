# potential_part_promotion.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Fetching data from MySQL
nation_query = "SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME = 'CANADA';"
mysql_cursor.execute(nation_query)
nation_result = mysql_cursor.fetchall()
nation_df = pd.DataFrame(nation_result, columns=['N_NATIONKEY', 'N_NAME'])

supplier_query = "SELECT S_SUPPKEY, S_NAME FROM supplier WHERE S_NATIONKEY = %s;"
supplier_df_list = []
for nation_key in nation_df['N_NATIONKEY']:
    mysql_cursor.execute(supplier_query, (nation_key,))
    supplier_result = mysql_cursor.fetchall()
    supplier_df = pd.DataFrame(supplier_result, columns=['S_SUPPKEY', 'S_NAME'])
    supplier_df_list.append(supplier_df)
supplier_df = pd.concat(supplier_df_list, ignore_index=True)

# Fetching data from MongoDB
partsupp = mongo_db['partsupp']
partsupp_df = pd.DataFrame(list(partsupp.find()))

# Fetch lineitem data from Redis
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Data pre-processing and filtering
lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1994-01-01') & (lineitem_df['L_SHIPDATE'] < '1995-01-01')]
lineitem_df = lineitem_df.merge(partsupp_df, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
lineitem_df = lineitem_df.merge(supplier_df, how='inner', on='S_SUPPKEY')

# Aggregating and filtering results
agg_df = lineitem_df.groupby(['S_SUPPKEY', 'S_NAME']).agg(Total_Quantity=('L_QUANTITY', 'sum')).reset_index()
agg_df['Excess'] = agg_df['Total_Quantity'] > agg_df['Total_Quantity'].mean() * 0.5

# Selecting suppliers with an excess of parts
suppliers_excess_df = agg_df[agg_df['Excess']]

# Write result to CSV
suppliers_excess_df.to_csv('query_output.csv', index=False)
