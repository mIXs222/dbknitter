import pymysql
import pandas as pd
from sqlalchemy import create_engine

# create connection
conn = pymysql.connect(host='mysql',
                       port=3306,
                       user='root',
                       password='my-secret-pw',
                       db='tpch')

# read data from mysql
lineitem = pd.read_sql('SELECT * FROM lineitem', conn)
part = pd.read_sql('SELECT * FROM part', conn)
conn.close()

# perform the query
merged_df = pd.merge(lineitem, part, left_on='L_PARTKEY', right_on='P_PARTKEY')
filtered_df = merged_df[(merged_df['P_BRAND'] == 'Brand#23') & (merged_df['P_CONTAINER'] == 'MED BAG')]
grouped_df = pd.DataFrame(filtered_df.groupby('P_PARTKEY')['L_QUANTITY'].mean() * 0.2)
filtered_df = filtered_df.merge(grouped_df, left_on='P_PARTKEY', right_on='P_PARTKEY', suffixes=('_left', '_right'))
filtered_df = filtered_df[filtered_df['L_QUANTITY_left'] < filtered_df['L_QUANTITY_right']]

result = pd.DataFrame({'AVG_YEARLY': [filtered_df['L_EXTENDEDPRICE'].sum() / 7.0]})
result.to_csv('query_output.csv', index=False)

