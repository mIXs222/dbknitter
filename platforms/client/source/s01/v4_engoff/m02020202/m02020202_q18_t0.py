# Save this code in a file named `execute_query.py`
import pymysql
import pandas as pd
import direct_redis

def get_mysql_orders_data():
    connection = pymysql.connect(
        host='mysql', database='tpch',
        user='root', password='my-secret-pw'
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE "
                "FROM orders WHERE O_TOTALPRICE > 300"
            )
            orders_data = cursor.fetchall()
    finally:
        connection.close()
    return orders_data

def get_redis_customers_data():
    redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    customers_data = pd.read_json(redis_connection.get('customer'))
    return customers_data

def main():
    orders_data = get_mysql_orders_data()
    customers_data = get_redis_customers_data()

    orders_df = pd.DataFrame(orders_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

    merged_data = pd.merge(customers_data, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    result = merged_data[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']]

    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
