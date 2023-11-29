# suppliers_who_kept_orders_waiting.py

import pymysql
import direct_redis
import pandas as pd

# MySQL connection and query
def get_mysql_data():
    mysql_connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )
    try:
        mysql_query = """
            SELECT s.S_NAME, l.L_ORDERKEY
            FROM lineitem l
            INNER JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
            WHERE s.S_NATIONKEY = (SELECT N_NATIONKEY FROM nation WHERE N_NAME = 'SAUDI ARABIA') 
              AND l.L_RETURNFLAG = 'F' 
              AND l.L_COMMITDATE < l.L_RECEIPTDATE
        """
        lineitems = pd.read_sql(mysql_query, con=mysql_connection)
        return lineitems
    finally:
        mysql_connection.close()

# Redis connection and data retrieval
def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    nations = pd.read_json(r.get('nation'))
    orders = pd.read_json(r.get('orders'))
    saudi_arabia_key = nations.loc[nations['N_NAME'] == 'SAUDI ARABIA', 'N_NATIONKEY'].values[0]
    filtered_orders = orders[orders['O_ORDERSTATUS'] == 'F']
    return saudi_arabia_key, filtered_orders

def main():
    saudi_arabia_key, redis_orders = get_redis_data()
    mysql_lineitems = get_mysql_data()

    # Filter orders for those with 'F' status and from SAUDI ARABIA
    orders_filtered = redis_orders[redis_orders['O_ORDERKEY'].isin(mysql_lineitems['L_ORDERKEY'])]

    # Check for multi-supplier orders and count waiting orders
    order_supplier_count = mysql_lineitems.groupby('L_ORDERKEY').size().reset_index(name='SUPPLIER_COUNT')
    multi_supplier_orders = order_supplier_count[order_supplier_count['SUPPLIER_COUNT'] > 1]['L_ORDERKEY']

    # Filter lineitems for multi-supplier orders and with SAUDI ARABIA nation key
    lineitems_multi_supplier = mysql_lineitems[
        (mysql_lineitems['L_ORDERKEY'].isin(multi_supplier_orders)) &
        (mysql_lineitems['L_ORDERKEY'].isin(orders_filtered['O_ORDERKEY']))
    ]
    
    # Count and sort the number of waiting lineitems
    result = lineitems_multi_supplier.groupby('S_NAME').size().reset_index(name='NUMWAIT')
    result = result.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

    # Save the result to CSV
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
