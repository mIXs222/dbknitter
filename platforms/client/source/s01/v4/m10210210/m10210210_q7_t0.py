import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)
mysql_cursor = mysql_conn.cursor()

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load Redis data into DataFrame
customer_df = pd.DataFrame(redis_client.get('customer'))

# Execute MySQL queries and load into DataFrames
mysql_cursor.execute("SELECT * FROM lineitem WHERE L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'")
lineitem_df = pd.DataFrame(mysql_cursor.fetchall(), columns=[i[0] for i in mysql_cursor.description])

# Load MongoDB data into DataFrames
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))
orders_df = pd.DataFrame(list(mongo_db.orders.find()))

# Merge DataFrames
merged_df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY') \
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY') \
    .merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY') \
    .merge(nation_df.add_prefix('S_'), left_on='S_NATIONKEY', right_on='S_N_NATIONKEY') \
    .merge(nation_df.add_prefix('C_'), left_on='C_NATIONKEY', right_on='C_N_NATIONKEY')

# Filter and rename columns to match those in SQL query
filtered_df = merged_df.query("(S_N_NAME == 'JAPAN' and C_N_NAME == 'INDIA') or (S_N_NAME == 'INDIA' and C_N_NAME == 'JAPAN')") \
    .rename(columns={'S_N_NAME': 'SUPP_NATION', 'C_N_NAME': 'CUST_NATION',
                     'L_SHIPDATE': 'L_YEAR', 'L_EXTENDEDPRICE': 'EXTENDEDPRICE', 'L_DISCOUNT': 'DISCOUNT'}) \
    .assign(L_YEAR=lambda x: pd.to_datetime(x['L_YEAR']).dt.year,
            VOLUME=lambda x: x['EXTENDEDPRICE'] * (1 - x['DISCOUNT']))

# Aggregate Data
result_df = filtered_df.groupby(['SUPP_NATION', 'CUST_NATION', 'L_YEAR'], as_index=False) \
    .agg({'VOLUME': 'sum'}) \
    .sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Save to CSV
result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
