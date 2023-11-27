import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

def get_mysql_data():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        cursorclass=pymysql.cursors.Cursor
    )
    try:
        with connection.cursor() as cursor:
            mysql_query = """
            SELECT
                L_ORDERKEY,
                L_EXTENDEDPRICE,
                L_DISCOUNT,
                L_SHIPDATE
            FROM
                lineitem
            """
            cursor.execute(mysql_query)
            lineitem_data = cursor.fetchall()
    finally:
        connection.close()
    return lineitem_data

def get_mongodb_data():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    customer_data = list(db.customer.find(
        {"C_MKTSEGMENT": "BUILDING"},
        {"_id": 0, "C_CUSTKEY": 1}
    ))
    client.close()
    return customer_data

def get_redis_data():
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    orders_data = redis_client.get('orders')
    redis_client.close()
    return orders_data

def process_data(mysql_data, mongodb_data, redis_data):
    lineitem_df = pd.DataFrame(mysql_data, columns=['L_ORDERKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_SHIPDATE'])
    customer_df = pd.DataFrame(mongodb_data)
    orders_df = pd.read_from_dict(redis_data)

    # Filter orders_df with O_ORDERDATE less than '1995-03-15'
    orders_df = orders_df[orders_df['O_ORDERDATE'] < '1995-03-15']

    # Merge DataFrames based on the given conditions
    merged_df = orders_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    merged_df = merged_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

    # Filter on L_SHIPDATE condition
    merged_df = merged_df[merged_df['L_SHIPDATE'] > '1995-03-15']

    # Calculate REVENUE
    merged_df['REVENUE'] = merged_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1)

    # Perform the grouping and sorting as in the SQL query
    result_df = merged_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY']) \
                         .agg({'REVENUE': 'sum'}) \
                         .reset_index() \
                         .sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

    return result_df[['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

def main():
    mysql_data = get_mysql_data()
    mongodb_data = get_mongodb_data()
    redis_data = get_redis_data()

    result_df = process_data(mysql_data, mongodb_data, redis_data)

    result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)

if __name__ == "__main__":
    main()
