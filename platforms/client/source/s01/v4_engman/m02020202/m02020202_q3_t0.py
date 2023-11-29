import pymysql
import pandas as pd
from direct_redis import DirectRedis

def execute_query():
    # MySQL connection
    mysql_conn = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        database='tpch'
    )

    # Redis connection
    redis_conn = DirectRedis(host='redis', port=6379, db=0)

    try:
        # Fetch orders data from MySQL
        with mysql_conn.cursor() as cursor:
            cursor.execute("""
                SELECT O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
                FROM orders
                WHERE O_ORDERDATE < '1995-03-05'
            """)
            orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])

        # Fetch lineitem data from Redis
        lineitem_df = pd.read_json(redis_conn.get('lineitem'))

        # Calculate revenue
        lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

        # Merge with orders dataframe on the O_ORDERKEY
        result = pd.merge(
            orders,
            lineitem_df,
            left_on='O_ORDERKEY',
            right_on='L_ORDERKEY',
            how='inner'
        )
        
        # Filter the result with the given conditions
        result = result[
            (result['L_SHIPDATE'] > '1995-03-15') &
            (result['REVENUE'] > 0)
        ]

        # Fetch customer data from Redis
        customer_df = pd.read_json(redis_conn.get('customer'))
        
        # Filter customers from 'BUILDING' market segment
        customer_building = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']

        # Merge with result data on C_CUSTKEY
        final_result = pd.merge(
            result,
            customer_building,
            left_on='O_CUSTKEY',
            right_on='C_CUSTKEY',
            how='inner'
        )

        # Select and sort the desired output
        final_output = final_result[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
        final_output = final_output.sort_values(by='REVENUE', ascending=False)

        # Write result to csv
        final_output.to_csv('query_output.csv', index=False)

    finally:
        mysql_conn.close()
        redis_conn.connection_pool.disconnect()

if __name__ == '__main__':
    execute_query()
