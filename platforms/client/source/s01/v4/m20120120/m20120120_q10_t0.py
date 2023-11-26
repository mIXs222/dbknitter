import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Function to fetch data from MySQL
def fetch_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    query = """
    SELECT
        L_ORDERKEY,
        L_EXTENDEDPRICE,
        L_DISCOUNT
    FROM
        lineitem
    WHERE
        L_RETURNFLAG = 'R'
    """
    df_mysql = pd.read_sql_query(query, connection)
    connection.close()
    return df_mysql

# Function to fetch data from MongoDB
def fetch_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    customer_data = list(db['customer'].find({}, {
        '_id': 0,
        'C_CUSTKEY': 1,
        'C_NAME': 1,
        'C_ACCTBAL': 1,
        'C_ADDRESS': 1,
        'C_PHONE': 1,
        'C_COMMENT': 1,
        'C_NATIONKEY': 1
    }))
    df_mongo = pd.DataFrame(customer_data)
    client.close()
    return df_mongo

# Function to fetch data from Redis
def fetch_redis_data():
    redis = DirectRedis(host='redis', port=6379, db=0)
    df_nation = pd.read_json(redis.get('nation'), orient='records')
    df_orders = pd.read_json(redis.get('orders'), orient='records')
    return df_nation, df_orders

# Fetch data from each source
lineitem = fetch_mysql_data()
customer = fetch_mongodb_data()
nation, orders = fetch_redis_data()

# Filtering orders by date
orders_filtered = orders[
    (pd.to_datetime(orders['O_ORDERDATE']) >= datetime(1993, 10, 1)) &
    (pd.to_datetime(orders['O_ORDERDATE']) < datetime(1994, 1, 1))
]

# Merging the data from different sources
merged_data = (
    orders_filtered
    .merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    .merge(customer, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')
)

# Calculating revenue
merged_data['REVENUE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])

# Grouping the data
grouped_data = merged_data.groupby(
    ['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'],
    as_index=False)['REVENUE'].sum()

# Ordering the data
result_data = grouped_data.sort_values(
    ['REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'],
    ascending=[False, True, True, False]
)

# Writing the results to a CSV file
result_data.to_csv('query_output.csv', index=False)
