# execute_query.py
import pandas as pd
import pymysql
import direct_redis

def connect_mysql():
    return pymysql.connect(
        host='mysql',
        port=3306,
        user='root',
        password='my-secret-pw',
        db='tpch'
    )

# Connect to MySQL and retrieve data
def get_mysql_data():
    try:
        conn = connect_mysql()
        with conn.cursor() as cursor:
            # Fetch the nation information from MySQL
            cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_NAME='SAUDI ARABIA'")
            nation_info = cursor.fetchone()
            nation_key = nation_info[0] if nation_info else None

            if nation_key is None:
                return pd.DataFrame()

            # Fetch the orders information from MySQL
            cursor.execute("""
                SELECT orders.O_ORDERKEY, orders.O_ORDERSTATUS
                FROM orders
                WHERE orders.O_ORDERSTATUS = 'F'
            """)
            orders_data = cursor.fetchall()
            orders_df = pd.DataFrame(orders_data, columns=['O_ORDERKEY', 'O_ORDERSTATUS'])

            return orders_df
    finally:
        conn.close()

# Connect to Redis and retrieve data
def get_redis_data():
    dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    # Fetch supplier and lineitem data
    suppliers_df = pd.read_json(dr.get('supplier'))
    lineitems_df = pd.read_json(dr.get('lineitem'))
    return suppliers_df, lineitems_df

def main():
    # Load orders and nation from MySQL
    orders_df = get_mysql_data()

    if orders_df.empty:
        print("No results found for nation 'SAUDI ARABIA'")
        return

    # Load supplier and lineitem from Redis
    suppliers_df, lineitems_df = get_redis_data()
    
    # Filter suppliers from SAUDI ARABIA and join with lineitems on S_SUPPKEY
    suppliers_df = suppliers_df[suppliers_df['S_NATIONKEY'] == nation_key]
    results_df = lineitems_df.merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY', how='inner')
    
    # Join with orders having status 'F'
    results_df = results_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')

    # Check if current supplier is the only one not meeting the ship date
    results_df['SUPPLIER_MET_COMMIT'] = results_df['L_COMMITDATE'] >= results_df['L_RECEIPTDATE']
    failed_suppliers_df = results_df.groupby('L_ORDERKEY').filter(lambda x: not x['SUPPLIER_MET_COMMIT'].any())

    # Select distinct suppliers
    distinct_suppliers_df = failed_suppliers_df[['S_SUPPKEY', 'S_NAME']].drop_duplicates()

    # Write results to query_output.csv
    distinct_suppliers_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
