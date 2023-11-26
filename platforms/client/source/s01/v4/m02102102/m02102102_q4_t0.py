# query.py
import pymysql
import pandas as pd
from direct_redis import DirectRedis

def get_mysql_orders_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT O_ORDERKEY, O_ORDERPRIORITY, O_ORDERDATE
            FROM orders
            WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
            """
            cursor.execute(query)
            order_data = cursor.fetchall()
    finally:
        connection.close()
    return pd.DataFrame(order_data, columns=['O_ORDERKEY', 'O_ORDERPRIORITY', 'O_ORDERDATE'])

def get_redis_lineitem_data():
    redis_client = DirectRedis(host='redis', port=6379)
    lineitem_data = pd.DataFrame(redis_client.get('lineitem'))
    lineitem_data = lineitem_data.loc[lineitem_data['L_COMMITDATE'] < lineitem_data['L_RECEIPTDATE']]
    return lineitem_data

def main():
    orders_df = get_mysql_orders_data()
    lineitem_df = get_redis_lineitem_data()

    # Merge the dataframes on L_ORDERKEY and O_ORDERKEY
    merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

    # Perform the aggregation to match the desired query output
    result = merged_df.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

    # Sort the result by O_ORDERPRIORITY
    result_sorted = result.sort_values(by='O_ORDERPRIORITY')

    # Write to CSV
    result_sorted.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
