# python_code.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Date range
start_date = datetime.strptime('1993-10-01', '%Y-%m-%d')
end_date = datetime.strptime('1993-12-31', '%Y-%m-%d')

# MySQL connection and query
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_query = """
    SELECT C_CUSTKEY, C_NAME, C_ACCTBAL, C_ADDRESS, C_PHONE, C_COMMENT
    FROM customer
"""
customer_df = pd.read_sql(mysql_query, mysql_conn)
mysql_conn.close()

# MongoDB connection and query for orders and nation
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']
nation_collection = mongodb['nation']

# MongoDB filters
order_filter = {
    'O_ORDERDATE': {
        '$gte': start_date,
        '$lte': end_date
    }
}
orders_df = pd.DataFrame(list(orders_collection.find(order_filter)))
nation_df = pd.DataFrame(list(nation_collection.find({})))

# Redis connection
redis_conn = DirectRedis(host='redis', port=6379, db=0)
lineitem_data = redis_conn.get('lineitem')
lineitem_df = pd.DataFrame(lineitem_data)

# Filter the lineitem table
lineitem_df = lineitem_df[lineitem_df['L_RETURNFLAG'] == 'R']

# Merge data from different databases
merged_df = customer_df.merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
merged_df = merged_df.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate REVENUE and group by customer attributes
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
result = merged_df.groupby(["C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "N_NAME", "C_ADDRESS", "C_COMMENT"]).agg({'REVENUE': 'sum'})

# Sort the results
result = result.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME', '-C_ACCTBAL'], ascending=[True, True, True, False])

# Saving the results to query_output.csv
result.to_csv("query_output.csv")

