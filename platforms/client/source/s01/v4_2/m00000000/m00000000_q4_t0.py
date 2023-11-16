import pymysql
import pandas as pd

def get_data_from_mysql(query):
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    df = pd.read_sql(query, connection)
    connection.close()
    return df

orders_query = """SELECT * FROM orders WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'"""
lineitem_query = """SELECT * FROM lineitem WHERE L_COMMITDATE < L_RECEIPTDATE"""

orders_df = get_data_from_mysql(orders_query)
lineitem_df = get_data_from_mysql(lineitem_query)

combined_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

final_df = combined_df.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].count().reset_index()
final_df.columns = ['O_ORDERPRIORITY', 'ORDER_COUNT']
final_df.sort_values('O_ORDERPRIORITY', inplace=True)

final_df.to_csv('query_output.csv', index=False)
