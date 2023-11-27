import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql', 
    user='root', 
    password='my-secret-pw', 
    database='tpch'
)
with mysql_conn.cursor() as cursor:
    # Get orders within the specified date range
    cursor.execute("""
        SELECT O_CUSTKEY, O_ORDERKEY, SUM(O_TOTALPRICE) AS revenue_lost 
        FROM orders 
        WHERE O_ORDERDATE >= '1993-10-01' AND O_ORDERDATE < '1994-01-01' 
        GROUP BY O_CUSTKEY, O_ORDERKEY
    """)
    orders_within_range = cursor.fetchall()
order_df = pd.DataFrame(orders_within_range, columns=['O_CUSTKEY', 'O_ORDERKEY', 'revenue_lost'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_col = mongo_db['customer']
customers = list(customer_col.find({}, {
    'C_CUSTKEY': 1, 'C_NAME': 1, 'C_ADDRESS': 1, 'C_NATIONKEY': 1, 
    'C_PHONE': 1, 'C_ACCTBAL': 1, 'C_COMMENT': 1
}))
customer_df = pd.DataFrame(customers)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
lineitems_str = redis_client.get('lineitem')
lineitems_df = pd.read_json(lineitems_str)

# Filter lineitems and calculate revenue lost post discount
lineitems_df['revenue_lost'] = lineitems_df['L_EXTENDEDPRICE'] * (1 - lineitems_df['L_DISCOUNT'])
lineitems_df = lineitems_df.groupby(['L_ORDERKEY'], as_index=False).agg({'revenue_lost': 'sum'})

# Join the tables
result_df = order_df \
    .merge(lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY') \
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Select and sort the final columns
result_df['revenue_lost'] = result_df['revenue_lost_x']
result_df = result_df[[
    'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 
    'C_COMMENT', 'revenue_lost'
]].sort_values(
    by=['revenue_lost', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], 
    ascending=[False, True, True, True]
)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
