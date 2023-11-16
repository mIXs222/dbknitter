# query.py

import pymysql
import pandas as pd


def handle_query(host, db, user, password, query):
    conn = pymysql.connect(host=host, user=user, password=password, db=db)
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
        
    return df


def write_to_csv(df, filename):
    df.to_csv(filename, index=False)


orders_query = """SELECT * FROM orders"""
lineitem_query = """SELECT * FROM lineitem"""

orders_df = handle_query('mysql', 'tpch', 'root', 'my-secret-pw', orders_query)
lineitem_df = handle_query('mysql', 'tpch', 'root', 'my-secret-pw', lineitem_query)

# Perform join operation
join_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter rows based on the conditions
cond1 = join_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])
cond2 = join_df['L_COMMITDATE'] < join_df['L_RECEIPTDATE']
cond3 = join_df['L_SHIPDATE'] < join_df['L_COMMITDATE']
cond4 = (join_df['L_RECEIPTDATE'] >= '1994-01-01') & (join_df['L_RECEIPTDATE'] < '1995-01-01')
filtered_df = join_df[cond1 & cond2 & cond3 & cond4]

# Generate the columns
filtered_df['HIGH_LINE_COUNT'] = (filtered_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])).astype(int)
filtered_df['LOW_LINE_COUNT'] = (~filtered_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])).astype(int)

# Group by L_SHIPMODE and compute the sum
grouped_df = filtered_df.groupby('L_SHIPMODE')[['HIGH_LINE_COUNT', 'LOW_LINE_COUNT']].sum().reset_index()

# Order by L_SHIPMODE
grouped_df = grouped_df.sort_values(by='L_SHIPMODE')

write_to_csv(grouped_df, 'query_output.csv')
