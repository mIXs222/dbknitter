# query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

def get_mysql_data(conn_info):
    connection = pymysql.connect(
        host=conn_info['hostname'],
        user=conn_info['username'],
        password=conn_info['password'],
        db=conn_info['database'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
    )

    try:
        with connection.cursor() as cursor:
            # Join nation and supplier to get suppliers from INDIA
            query = """
                SELECT s.S_SUPPKEY, n.N_NAME
                FROM supplier s
                JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
                WHERE n.N_NAME = 'INDIA'
            """
            cursor.execute(query)
            suppliers_from_india = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'N_NAME'])
            
            # Get orders from 1995 and 1996
            query = """
                SELECT o.O_ORDERKEY, YEAR(o.O_ORDERDATE) AS Year, o.O_TOTALPRICE
                FROM orders o
                WHERE YEAR(o.O_ORDERDATE) IN (1995, 1996)
            """
            cursor.execute(query)
            orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'Year', 'O_TOTALPRICE'])
        
        return (suppliers_from_india, orders)
    finally:
        connection.close()

def get_mongo_data(conn_info):
    client = pymongo.MongoClient(conn_info['hostname'], conn_info['port'])
    db = client[conn_info['database']]
    
    # Get parts that are SMALL PLATED COPPER
    parts_cursor = db.part.find({"P_TYPE": "SMALL PLATED COPPER"}, {"P_PARTKEY": 1})
    parts = pd.DataFrame(list(parts_cursor))
    
    return parts

def get_redis_data(conn_info):
    redis_client = DirectRedis(host=conn_info['hostname'], port=conn_info['port'], db=int(conn_info['database']))
    
    # Get line items and convert to DataFrame
    lineitems = pd.read_csv(redis_client.get('lineitem'))
    
    return lineitems

def calculate_market_share(suppliers, orders, parts, lineitems):
    # Perform necessary joins
    df = (
        lineitems
        .merge(parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
        .merge(orders, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
        .merge(suppliers, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    )
    
    # Calculate revenue
    df['revenue'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
    
    # Group by year and calculate market share for INDA in ASIA
    market_share = df.groupby('Year')['revenue'].sum().reset_index()
    
    return market_share

# Connection information for each database
mysql_conn_info = {
    'database': 'tpch',
    'username': 'root',
    'password': 'my-secret-pw',
    'hostname': 'mysql'
}

mongodb_conn_info = {
    'database': 'tpch',
    'port': 27017,
    'hostname': 'mongodb'
}

redis_conn_info = {
    'database': '0',
    'port': 6379,
    'hostname': 'redis'
}

# Get data from each database
(suppliers_from_india, orders) = get_mysql_data(mysql_conn_info)
parts = get_mongo_data(mongodb_conn_info)
lineitems = get_redis_data(redis_conn_info)

# Calculate market share
market_share_result = calculate_market_share(suppliers_from_india, orders, parts, lineitems)

# Write result to csv
market_share_result.to_csv('query_output.csv', index=False)
