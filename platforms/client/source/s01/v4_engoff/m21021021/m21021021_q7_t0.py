import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# MySQL Connection
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()
mysql_cursor.execute(
    """
    SELECT C_CUSTKEY, C_NATIONKEY 
    FROM customer 
    WHERE C_NATIONKEY IN (SELECT N_NATIONKEY FROM nation WHERE N_NAME='INDIA' OR N_NAME='JAPAN')
    """
)
customers = pd.DataFrame(mysql_cursor.fetchall(), columns=['C_CUSTKEY', 'C_NATIONKEY'])
mysql_cursor.close()
mysql_conn.close()

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
lineitems = pd.DataFrame(list(mongo_db.lineitem.find(
    {'L_SHIPDATE': {'$gte': datetime(1995, 1, 1), '$lte': datetime(1996, 12, 31)}},
    projection={'_id': False}
)))

# Redis Connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))

# Data Processing
suppliers_info = supplier_df[supplier_df['S_NATIONKEY'].isin(nation_df[nation_df['N_NAME'].isin(['INDIA', 'JAPAN'])]['N_NATIONKEY'])]
nation_map = nation_df.set_index('N_NATIONKEY').to_dict()['N_NAME']

# Combine data
results = pd.merge(lineitems, suppliers_info, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
results = pd.merge(results, customers, left_on='L_ORDERKEY', right_on='C_CUSTKEY')
results = results[(results['C_NATIONKEY'] != results['S_NATIONKEY']) \
                  & ((results['C_NATIONKEY'].isin(nation_map)) & (results['S_NATIONKEY'].isin(nation_map)))]

# Calculate revenue
results['YEAR'] = results['L_SHIPDATE'].dt.year
results['REVENUE'] = results['L_EXTENDEDPRICE'] * (1 - results['L_DISCOUNT'])
results = results.groupby(['S_NATIONKEY', 'C_NATIONKEY', 'YEAR']).agg({'REVENUE': 'sum'}).reset_index()

# Add nation names
results['SUPPLIER_NATION'] = results['S_NATIONKEY'].map(nation_map)
results['CUSTOMER_NATION'] = results['C_NATIONKEY'].map(nation_map)

# Select and order results
results = results[['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR', 'REVENUE']]
results.sort_values(['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR'], inplace=True)

# Write to CSV
results.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
