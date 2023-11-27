# File: run_query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

def connect_mysql(host, user, password, db):
    return pymysql.connect(host=host, user=user, password=password, db=db)

def connect_mongodb(host, port, db):
    return pymongo.MongoClient(host=host, port=port)[db]

def connect_redis(host, port, db):
    return DirectRedis(host=host, port=port, db=db)

def query_mysql(conn, year):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as revenue
            FROM 
                lineitem l
            JOIN 
                orders o ON l.L_ORDERKEY = o.O_ORDERKEY
            JOIN 
                supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
            JOIN 
                nation n ON s.S_NATIONKEY = n.N_NATIONKEY
            JOIN 
                region r ON n.N_REGIONKEY = r.R_REGIONKEY
            JOIN 
                part p ON p.P_PARTKEY = l.L_PARTKEY
            WHERE 
                r.R_NAME = 'ASIA' AND 
                n.N_NAME = 'INDIA' AND 
                p.P_TYPE = 'SMALL PLATED COPPER' AND 
                YEAR(o.O_ORDERDATE) = {year}
        """)
        result = cursor.fetchone()
        return result[0] if result[0] is not None else 0

def main():
    # Connect to databases
    mysql_conn = connect_mysql(host='mysql', user='root', password='my-secret-pw', db='tpch')
    mongo_db = connect_mongodb(host='mongodb', port=27017, db='tpch')
    redis_conn = connect_redis(host='redis', port=6379, db=0)

    # Perform queries for each year
    revenue_1995 = query_mysql(mysql_conn, 1995)
    revenue_1996 = query_mysql(mysql_conn, 1996)

    # Get ASIA and INDIA keys from Redis
    # Assume nation and region tables are stored as hashmaps in Redis
    asia_key = redis_conn.hget('region', 'ASIA')
    india_key = redis_conn.hget('nation', 'INDIA')

    # Calculate total revenues for ASIA and INDIA by querying MongoDB for orders
    orders = mongo_db.orders.find({
        'O_ORDERDATE': {'$gte': pd.Timestamp(year=1995), '$lt': pd.Timestamp(year=1997)}
    })
    # Assuming lineitem is an embedded document inside orders
    asia_revenues = sum(
        order['L_EXTENDEDPRICE'] * (1 - order['L_DISCOUNT']) 
        for order in orders
        if order['R_REGIONKEY'] == asia_key and 
           order['N_NATIONKEY'] == india_key and 
           order['P_TYPE'] == 'SMALL PLATED COPPER'
    )

    # Calculate market share for each year
    market_share_1995 = revenue_1995 / asia_revenues if asia_revenues != 0 else 0
    market_share_1996 = revenue_1996 / asia_revenues if asia_revenues != 0 else 0   

    # Write the results to a CSV
    results = pd.DataFrame({
        'Year': ['1995', '1996'],
        'Market Share': [market_share_1995, market_share_1996]
    })
    results.to_csv('query_output.csv', index=False)

    # Close connections
    mysql_conn.close()

if __name__ == '__main__':
    main()
