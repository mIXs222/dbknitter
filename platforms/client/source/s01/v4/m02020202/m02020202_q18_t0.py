# query.py

import pandas as pd
import pymysql
import direct_redis

def execute_query_mysql():
    connection_info = {
        "host": "mysql",
        "user": "root",
        "password": "my-secret-pw",
        "db": "tpch",
        "charset": "utf8mb4"
    }

    try:
        connection = pymysql.connect(**connection_info)
        with connection.cursor() as cursor:
            query_orders = """
                SELECT
                    O_ORDERKEY,
                    O_CUSTKEY,
                    O_ORDERDATE,
                    O_TOTALPRICE
                FROM
                    orders
                WHERE
                    O_ORDERKEY IN (
                        SELECT L_ORDERKEY
                        FROM lineitem
                        GROUP BY L_ORDERKEY
                        HAVING SUM(L_QUANTITY) > 300
                    )
            """
            cursor.execute(query_orders)
            orders_result = cursor.fetchall()

            return pd.DataFrame(orders_result, columns=["O_ORDERKEY", "O_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"])

    finally:
        if connection:
            connection.close()

def get_data_redis():
    dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    customer_df = dr.get('customer')
    lineitem_df = dr.get('lineitem')
    return customer_df, lineitem_df

if __name__ == '__main__':
    orders_df = execute_query_mysql()
    customer_df, lineitem_df = get_data_redis()
    
    # Merge lineitem with orders
    lineitem_filtered = lineitem_df[lineitem_df['L_ORDERKEY'].isin(orders_df['O_ORDERKEY'].values)]
    lineitem_agg = lineitem_filtered.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()
    
    # Merge lineitem_agg with orders dataframe
    combined_df = pd.merge(orders_df, lineitem_agg, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    
    # Further merge with customer dataframe
    result = pd.merge(combined_df, customer_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    
    # Select specified columns
    output_df = result.loc[:, ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]
    
    # Group by
    final_df = output_df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'L_QUANTITY': 'sum'}).reset_index()
    
    # Order by
    final_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)
    
    # Output to CSV
    final_df.to_csv('query_output.csv', index=False)
