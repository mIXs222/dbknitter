import pymysql
import pymongo
import pandas as pd
import datetime
from direct_redis import DirectRedis

# --- MySQL Connection and Query ---
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = '''
SELECT N_NAME, N_NATIONKEY FROM nation
'''
nation_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# --- MongoDB Connection and Query ---
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
orders = list(mongo_db["orders"].find({}, {'_id': False}))
lineitem = list(mongo_db["lineitem"].find(
    {
        'L_SHIPDATE': {'$gte': datetime.datetime(1995, 1, 1), '$lte': datetime.datetime(1996, 12, 31)}
    }, 
    {'_id': False})
)
orders_df = pd.DataFrame(orders)
lineitem_df = pd.DataFrame(lineitem)
mongo_client.close()

# --- Redis Connection and Query ---
redis_conn = DirectRedis(host='redis', port=6379, db=0)
supplier_data = redis_conn.get('supplier')
customer_data = redis_conn.get('customer')
supplier_df = pd.DataFrame(supplier_data)
customer_df = pd.DataFrame(customer_data)

# Merging the dataframes
df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
df = df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df = df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
df = df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df = df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_SUPP', '_CUST'))

# Filter for nation names and calculate volume
df['YEAR'] = pd.to_datetime(df['L_SHIPDATE']).dt.year
df['VOLUME'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
df_filtered = df[
    (
        ((df['N_NAME_SUPP'] == 'JAPAN') & (df['N_NAME_CUST'] == 'INDIA')) |
        ((df['N_NAME_SUPP'] == 'INDIA') & (df['N_NAME_CUST'] == 'JAPAN'))
    )
]

# Grouping and aggregating the result
result = df_filtered.groupby(['N_NAME_SUPP', 'N_NAME_CUST', 'YEAR']).agg(REVENUE=('VOLUME', 'sum')).reset_index()
result = result.rename(columns={'N_NAME_SUPP': 'SUPP_NATION', 'N_NAME_CUST': 'CUST_NATION', 'YEAR': 'L_YEAR'})
result = result.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Writing the result to a .csv file
result.to_csv('query_output.csv', index=False)
