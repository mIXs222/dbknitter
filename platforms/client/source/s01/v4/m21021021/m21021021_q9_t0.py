import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from functools import reduce
from datetime import datetime
import csv


# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Helper function to convert Redis data into a DataFrame
def redis_to_df(redis_data):
    return pd.DataFrame([eval(item) for item in redis_data])

# Fetch data from MySQL
mysql_query = """
SELECT P_PARTKEY, P_NAME
FROM part
WHERE P_NAME LIKE '%dim%'
"""
mysql_cursor.execute(mysql_query)
part_data = mysql_cursor.fetchall()
part_df = pd.DataFrame(part_data, columns=["P_PARTKEY", "P_NAME"])

# Fetch data from MongoDB
partsupp_df = pd.DataFrame(list(mongo_db.partsupp.find({}, {'_id': 0})))
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find({}, {'_id': 0})))

# Fetch data from Redis
nation_data = redis_client.get('nation')
supplier_data = redis_client.get('supplier')
orders_data = redis_client.get('orders')

nation_df = redis_to_df(nation_data)
supplier_df = redis_to_df(supplier_data)
orders_df = redis_to_df(orders_data)

# Convert order date to year and calculate amount
lineitem_df['O_YEAR'] = lineitem_df['L_SHIPDATE'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").year)
lineitem_df['AMOUNT'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT']) - partsupp_df.set_index('PS_PARTKEY')['PS_SUPPLYCOST'] * lineitem_df['L_QUANTITY']

# Merge DataFrames to get the required tables
df_merge = reduce(lambda left, right: pd.merge(left, right, on='S_SUPPKEY'),
                  [supplier_df.set_index('S_SUPPKEY'), lineitem_df.set_index('L_SUPPKEY')])

df_merge = pd.merge(df_merge.reset_index(), part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
df_merge = pd.merge(df_merge, orders_df, left_on='O_ORDERKEY', right_on='O_ORDERKEY')
df_merge = pd.merge(df_merge, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Group by NATION, O_YEAR
grouped_df = df_merge.groupby(['N_NAME', 'O_YEAR'])
result = grouped_df['AMOUNT'].sum().reset_index()
result.columns = ['NATION', 'O_YEAR', 'SUM_PROFIT']

# Sort and output to CSV
result.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
