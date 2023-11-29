# query.py
import pandas as pd
import pymysql
from pymongo import MongoClient
from direct_redis import DirectRedis
import csv

# Mysql Connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Fetching lineitem from MySQL
query = """
SELECT
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_SHIPDATE
FROM
    lineitem
WHERE
    L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31';
"""
lineitem_df = pd.read_sql(query, mysql_conn)
mysql_conn.close()

# Calculating revenue
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
lineitem_df['L_YEAR'] = pd.DatetimeIndex(lineitem_df['L_SHIPDATE']).year

# MongoDB Connection
mongo_client = MongoClient('mongodb', 27017)
db = mongo_client['tpch']

# Fetching customer from MongoDB
customers = list(db.customer.find({}, {'_id': 0}))
customer_df = pd.DataFrame(customers)
customer_df = customer_df.rename(columns={'C_CUSTKEY': 'CUSTOMER_KEY', 'C_NATIONKEY': 'CUST_NATION'})

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetching nation from Redis
nation_data = redis_client.get('nation')
nation_df = pd.read_json(nation_data, orient='records')
nation_df = nation_df.rename(columns={'N_NATIONKEY': 'NATION_KEY', 'N_NAME': 'NATION_NAME'})

# Fetching supplier from Redis
supplier_data = redis_client.get('supplier')
supplier_df = pd.read_json(supplier_data, orient='records')
supplier_df = supplier_df.rename(columns={'S_SUPPKEY': 'SUPPLIER_KEY', 'S_NATIONKEY': 'SUPP_NATION'})

# Joining dataframes
join_df = lineitem_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='SUPPLIER_KEY')
join_df = join_df.merge(customer_df, left_on='L_ORDERKEY', right_on='CUSTOMER_KEY')
join_df = nation_df.rename(columns={'NATION_KEY': 'SUPP_NATION'}).merge(join_df, on='SUPP_NATION')
join_df = nation_df.rename(columns={'NATION_KEY': 'CUST_NATION'}).merge(join_df, on='CUST_NATION')

# Filtering nations as per query (INDIA and JAPAN)
join_df = join_df[(join_df['NATION_NAME_x'] == 'INDIA') & (join_df['NATION_NAME_y'] == 'JAPAN') |
                  (join_df['NATION_NAME_x'] == 'JAPAN') & (join_df['NATION_NAME_y'] == 'INDIA')]

# Selecting relevant columns
result = join_df[['NATION_NAME_y', 'L_YEAR', 'REVENUE', 'NATION_NAME_x']]
result = result.rename(columns={'NATION_NAME_y': 'CUST_NATION', 'NATION_NAME_x': 'SUPP_NATION'})

# Sorting as per query
result = result.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Output to CSV
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
