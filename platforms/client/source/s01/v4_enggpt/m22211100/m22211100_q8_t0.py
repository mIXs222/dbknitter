import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis

# MySQL connection
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Function to get Redis DataFrame
def get_redis_df(table_name):
    data_str = redis_client.get(table_name)
    return pd.read_json(data_str)

# Query MySQL for orders and lineitem data
with mysql_conn.cursor() as cursor:
    cursor.execute("""
    SELECT o.O_ORDERDATE, l.L_EXTENDEDPRICE, l.L_DISCOUNT, l.L_ORDERKEY
    FROM lineitem l
    JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
    WHERE O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
    """)
    orders_lineitems_data = cursor.fetchall()

# Converting MySQL data to pandas DataFrame
orders_lineitems_df = pd.DataFrame(orders_lineitems_data, columns=['O_ORDERDATE', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_ORDERKEY'])

# Query MongoDB for customer data
customer_data = mongo_db.customer.find({})
customer_df = pd.DataFrame(list(customer_data))

# Query MongoDB for supplier data
supplier_data = mongo_db.supplier.find({})
supplier_df = pd.DataFrame(list(supplier_data))

# Get Redis DataFrame for nation, region, and part
nation_df = get_redis_df('nation')
region_df = get_redis_df('region')
part_df = get_redis_df('part')

# Join dataframes to get the required information
region_asia_df = region_df[region_df['R_NAME'] == 'ASIA']
nation_india_df = nation_df[nation_df['N_NAME'] == 'INDIA']

joined_df = orders_lineitems_df.merge(customer_df, left_on='L_ORDERKEY', right_on='C_CUSTKEY', how='inner')
joined_df = joined_df.merge(supplier_df, left_on='C_CUSTKEY', right_on='S_SUPPKEY', how='inner')
joined_df = joined_df.merge(part_df[part_df['P_TYPE'] == 'SMALL PLATED COPPER'], left_on='L_PARTKEY', right_on='P_PARTKEY', how='inner')
joined_df = joined_df.merge(nation_india_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY', how='inner')
joined_df = joined_df.merge(region_asia_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY', how='inner')

# Volume Calculation and Group by year
joined_df['year'] = pd.to_datetime(joined_df['O_ORDERDATE']).dt.year
joined_df['volume'] = joined_df['L_EXTENDEDPRICE'] * (1 - joined_df['L_DISCOUNT'])
summary_df = joined_df.groupby('year')['volume'].sum().reset_index()

# Calculate total volume
total_volume = joined_df['volume'].sum()

# Calculate market share
summary_df['market_share'] = summary_df['volume'] / total_volume

# Sort by year
final_df = summary_df.sort_values('year')

# Write to CSV
final_df.to_csv('query_output.csv', index=False)
