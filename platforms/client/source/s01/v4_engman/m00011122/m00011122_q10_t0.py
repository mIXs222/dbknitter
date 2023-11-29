import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Get nation data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation;")
    nations = cursor.fetchall()
nation_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get customer data from MongoDB
customer_data = mongo_db.customer.find({})
customer_df = pd.DataFrame(list(customer_data))

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Get orders data from Redis
orders_df = pd.read_hdf(f'direct_redis://{redis_conn}/orders')

# Filter orders by date
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= start_date) &
    (orders_df['O_ORDERDATE'] <= end_date)
]

# Get lineitem data from Redis
lineitem_df = pd.read_hdf(f'direct_redis://{redis_conn}/lineitem')

# Calculate revenue lost
lineitem_df['REVENUE_LOST'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Join and filter dataframes
result_df = (
    filtered_orders
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
)

# Group by to get the aggregate of revenue lost
summary_df = (
    result_df.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'])
    .agg({'REVENUE_LOST': 'sum'})
    .reset_index()
)

# Apply sorting
summary_df.sort_values(
    by=['REVENUE_LOST', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
    ascending=[True, True, True, False],
    inplace=True
)

# Write to CSV file
summary_df.to_csv('query_output.csv', index=False)
