# File name: execute_query.py
import pymysql
import pymongo
import pandas as pd
from sqlalchemy import create_engine

# MySQL connection setup
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

mysql_engine = create_engine(f"mysql+pymysql://root:my-secret-pw@mysql/tpch")

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']

# Read data from mysql
mysql_customer_df = pd.read_sql("SELECT * FROM customer", mysql_conn)
mysql_orders_df = pd.read_sql("SELECT * FROM orders", mysql_conn)
mysql_lineitem_df = pd.read_sql("SELECT * FROM lineitem", mysql_conn)

# Read data from mongodb
part_col = mongo_db['part']
supplier_col = mongo_db['supplier']
nation_col = mongo_db['nation']
region_col = mongo_db['region']

part_df = pd.DataFrame(list(part_col.find()))
supplier_df = pd.DataFrame(list(supplier_col.find()))
nation1_df = pd.DataFrame(list(nation_col.find()))
nation2_df = nation1_df.copy()
region_df = pd.DataFrame(list(region_col.find()))

# Close connections
mysql_conn.close()
mongo_client.close()

# Merge MySQL and MongoDB dataframes
merged_df = mysql_lineitem_df.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df[merged_df['P_TYPE'] == 'SMALL PLATED COPPER']
merged_df = merged_df.merge(supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(mysql_orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merged_df = merged_df.merge(mysql_customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(nation1_df, how='inner', left_on='C_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(region_df, how='inner', left_on='N_REGIONKEY', right_on='R_REGIONKEY')
merged_df = merged_df[merged_df['R_NAME'] == 'ASIA']
merged_df = merged_df.merge(nation2_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.rename(columns={'N_NAME_y': 'NATION'})

# Filtering by order date
merged_df['O_ORDERDATE'] = pd.to_datetime(merged_df['O_ORDERDATE'])
merged_df = merged_df[(merged_df['O_ORDERDATE'] >= '1995-01-01') & (merged_df['O_ORDERDATE'] <= '1996-12-31')]

# Calculate volume
merged_df['VOLUME'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate market share
result_df = merged_df.groupby(merged_df['O_ORDERDATE'].dt.year)['VOLUME'].apply(
    lambda x: (x[merged_df['NATION'] == 'INDIA'].sum()) / x.sum()
).reset_index()
result_df.columns = ['O_YEAR', 'MKT_SHARE']

# Save results to CSV file
result_df.to_csv('query_output.csv', index=False)
