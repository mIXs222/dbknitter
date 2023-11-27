# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

mongo_conn_info = {
    'host': 'mongodb',
    'port': 27017
}

redis_conn_info = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
# Connect to MongoDB
mongo_client = pymongo.MongoClient(**mongo_conn_info)
mongo_db = mongo_client['tpch']
# Connect to Redis
redis_client = DirectRedis(**redis_conn_info)

# For the given time range
start_date = "1993-10-01"
end_date = "1994-01-01"

try:
    # MySQL query
    with mysql_conn.cursor() as cursor:
        query_customers = """
        SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_COMMENT
        FROM customer
        """
        cursor.execute(query_customers)
        customers = pd.DataFrame(cursor.fetchall(), columns=["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_COMMENT"])

    # MongoDB query
    nations = pd.DataFrame(list(mongo_db.nation.find({}, {"_id": 0})))
    orders = pd.DataFrame(list(mongo_db.orders.find(
        {"O_ORDERDATE": {"$gte": datetime.strptime(start_date, "%Y-%m-%d"), "$lt": datetime.strptime(end_date, "%Y-%m-%d")}},
        {"_id": 0}
    )))

    # Redis query
    lineitems_raw = redis_client.get('lineitem')
    lineitems = pd.read_json(lineitems_raw, orient='split')

    # Merge orders with lineitems on O_ORDERKEY
    lineitems_orders = lineitems.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")

    # Calculate revenue lost
    lineitems_orders['LOST_REVENUE'] = lineitems_orders.apply(
        lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']),
        axis=1
    )
    
    # Get qualifying lineitems
    qualifying_lineitems = lineitems_orders[lineitems_orders['L_RETURNFLAG'] == 'R']

    # Group by customer key and sum the lost revenue
    revenue_lost_per_customer = qualifying_lineitems.groupby('O_CUSTKEY')['LOST_REVENUE'].sum().reset_index()

    # Merge customers with revenue_lost_per_customer on C_CUSTKEY
    customers_with_revenue = customers.merge(revenue_lost_per_customer, left_on="C_CUSTKEY", right_on="O_CUSTKEY")

    # Merge customers with nations on C_NATIONKEY
    result = customers_with_revenue.merge(nations, left_on="C_NATIONKEY", right_on="N_NATIONKEY")

    # Select required columns and sort as per the requirements
    final_result = result[[
        "C_NAME", "C_ADDRESS", "N_NAME", "C_PHONE", "C_ACCTBAL", "C_COMMENT", "LOST_REVENUE"
    ]].sort_values(
        by=["LOST_REVENUE", "C_CUSTKEY", "C_NAME", "C_ACCTBAL"],
        ascending=[False, True, True, True]
    )

    # Write to CSV
    final_result.to_csv('query_output.csv', index=False)

finally:
    # Close all connections
    mysql_conn.close()
    mongo_client.close()
