import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
from decimal import Decimal

# MySQL connection
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Helper function to handle decimal and datetime types in MongoDB
def handle_mongo_types(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')
    return value

# Helper function to execute MySQL queries and return data as DataFrame
def fetch_mysql(sql_query):
    with mysql_connection.cursor() as cursor:
        cursor.execute(sql_query)
        data = cursor.fetchall()
        columns = [col_desc[0] for col_desc in cursor.description]
        return pd.DataFrame(data, columns=columns)

# Helper function to execute Redis commands and return data as DataFrame
def fetch_redis(table_name):
    data_string = redis_connection.get(table_name)
    if data_string:
        data_list = eval(data_string)  # `eval` is used because Redis stores data as a string, be cautious using eval considering security implications
        return pd.DataFrame(data_list)
    return pd.DataFrame()

# Get data from MySQL
orders_dataframe = fetch_mysql("""
    SELECT o_orderkey, o_orderdate, SUM(l_extendedprice * (1 - l_discount)) AS volume
    FROM orders
    INNER JOIN lineitem ON o_orderkey = l_orderkey
    GROUP BY o_orderkey, o_orderdate
""")

nation_dataframe = fetch_mysql("""
    SELECT n_nationkey, n_name
    FROM nation
    WHERE n_name = 'INDIA'
""")

region_dataframe = fetch_redis('region')

# Get data from MongoDB
part_cursor = mongo_db.part.find(
    {"P_TYPE": "SMALL PLATED COPPER"},
    {"P_PARTKEY": 1, "_id": 0}
)
part_dataframe = pd.DataFrame((handle_mongo_types(doc) for doc in part_cursor))

customer_cursor = mongo_db.customer.find(
    {"C_REGIONKEY": {"$in": region_dataframe.loc[region_dataframe['R_NAME'] == 'ASIA', 'R_REGIONKEY'].tolist()}},
    {"C_CUSTKEY": 1, "_id": 0}
)
customer_dataframe = pd.DataFrame((handle_mongo_types(doc) for doc in customer_cursor))

# Combine data from different databases
combined_dataframe = orders_dataframe.merge(part_dataframe, left_on='l_partkey', right_on='p_partkey')
combined_dataframe = combined_dataframe.merge(customer_dataframe, left_on='o_custkey', right_on='c_custkey')
combined_dataframe = combined_dataframe.merge(nation_dataframe, left_on='n_nationkey', right_on='n_nationkey')

# Filter data by time
combined_dataframe['o_orderdate'] = pd.to_datetime(combined_dataframe['o_orderdate'])
combined_dataframe = combined_dataframe[
    (combined_dataframe['o_orderdate'].dt.year >= 1995) & (combined_dataframe['o_orderdate'].dt.year <= 1996)
]

# Calculate market share
india_volume = combined_dataframe[combined_dataframe['n_name'] == 'INDIA']['volume'].sum()
total_volume = combined_dataframe['volume'].sum()
market_share = india_volume / total_volume if total_volume else 0

# Group by order year
grouped_data = combined_dataframe.groupby(combined_dataframe['o_orderdate'].dt.year).agg({'volume': 'sum'})
grouped_data['market_share'] = grouped_data['volume'] / total_volume

# Sort the results
sorted_data = grouped_data.sort_index().reset_index()
sorted_data.columns = ['year_of_order', 'total_volume', 'market_share']

# Write to CSV
sorted_data.to_csv('query_output.csv', index=False)

# Close all connections
mysql_connection.close()
mongo_client.close()
redis_connection.close()
