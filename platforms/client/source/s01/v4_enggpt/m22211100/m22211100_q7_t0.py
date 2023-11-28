import pandas as pd
import pymysql
import pymongo
import direct_redis

# Connect to MySQL database
mysql_conn = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Connect to MongoDB
mongodb_client = pymongo.MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query for MySQL (orders and lineitem)
mysql_query = """
SELECT
    o.O_ORDERDATE,
    l.L_EXTENDEDPRICE,
    l.L_DISCOUNT,
    l.L_ORDERKEY
FROM
    orders AS o
INNER JOIN
    lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE
    o.O_ORDERDATE >= '1995-01-01' AND o.O_ORDERDATE <= '1996-12-31'
"""
orders_lineitem_df = pd.read_sql(mysql_query, mysql_conn)

# Computation for revenue volume
orders_lineitem_df['REVENUE'] = orders_lineitem_df['L_EXTENDEDPRICE'] * (1 - orders_lineitem_df['L_DISCOUNT'])
orders_lineitem_df['YEAR'] = pd.to_datetime(orders_lineitem_df['O_ORDERDATE']).dt.year

# Query for MongoDB (supplier and customer: fetching all, filtering to be done later)
suppliers = list(mongodb_db.supplier.find({}, {'_id': False}))
customers = list(mongodb_db.customer.find({}, {'_id': False}))

# Convert to pandas DataFrames
suppliers_df = pd.DataFrame(suppliers)
customers_df = pd.DataFrame(customers)

# nation: Read from Redis
nation_data = redis_conn.get('nation')

# If Redis data is not a DataFrame, convert it into one
if not isinstance(nation_data, pd.DataFrame):
    from io import StringIO
    nation_data = pd.read_csv(StringIO(nation_data.decode('utf-8')))

# Filtering nations of interest and necessary columns
interesting_nations = ['JAPAN', 'INDIA']
nation_data = nation_data[nation_data['N_NAME'].isin(interesting_nations)][['N_NATIONKEY', 'N_NAME']]
suppliers_df = suppliers_df[suppliers_df['S_NATIONKEY'].isin(nation_data['N_NATIONKEY'])]
customers_df = customers_df[customers_df['C_NATIONKEY'].isin(nation_data['N_NATIONKEY'])]

# Merge to get customer and supplier nations
merged_df = orders_lineitem_df.merge(suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(customers_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
merged_df = merged_df.merge(nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(nation_data, left_on='C_NATIONKEY', right_on='N_NATIONKEY', suffixes=('_SUPPLIER', '_CUSTOMER'))

# Filter for combinations of Japan and India supplier/customer
filtered_df = merged_df[
    ((merged_df['N_NAME_SUPPLIER'] == 'JAPAN') & (merged_df['N_NAME_CUSTOMER'] == 'INDIA')) |
    ((merged_df['N_NAME_SUPPLIER'] == 'INDIA') & (merged_df['N_NAME_CUSTOMER'] == 'JAPAN'))
]

# Group by supplier nation, customer nation, and year of shipping
grouped_df = filtered_df.groupby(['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'YEAR']).agg({
    'REVENUE': 'sum'}).reset_index()

# Sort results
sorted_df = grouped_df.sort_values(by=['N_NAME_SUPPLIER', 'N_NAME_CUSTOMER', 'YEAR'])

# Write to CSV
sorted_df.to_csv('query_output.csv', index=False)

# Clean up the connections
mysql_conn.close()
mongodb_client.close()
