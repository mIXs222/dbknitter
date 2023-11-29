import pymysql
import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis using DirectRedis to accommodate Pandas DataFrame
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Query for orders between 1990 and 1995 from MySQL
mysql_cursor.execute("""
    SELECT O_ORDERKEY, C_CUSTKEY, O_ORDERDATE
    FROM orders
    WHERE O_ORDERDATE BETWEEN '1990-01-01' AND '1995-01-01'
""")
orders = mysql_cursor.fetchall()

# Get the lineitems from MySQL
mysql_cursor.execute("SELECT * FROM lineitem")
lineitems = mysql_cursor.fetchall()

# Create dataframes from orders and lineitems
df_orders = pd.DataFrame(orders, columns=['O_ORDERKEY', 'C_CUSTKEY', 'O_ORDERDATE'])
df_lineitem = pd.DataFrame(lineitems, columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

# Filter lineitems using orders
filtered_lineitems = df_lineitem[df_lineitem['L_ORDERKEY'].isin(df_orders['O_ORDERKEY'])]

# Calculate revenue
filtered_lineitems['REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])

# Get suppliers from MongoDB
suppliers = list(mongo_db['supplier'].find())
df_suppliers = pd.DataFrame(suppliers)

# Get customers from MongoDB
customers = list(mongo_db['customer'].find({}, {'C_CUSTKEY': 1, 'C_NATIONKEY': 1}))
df_customers = pd.DataFrame(customers)

# Get nations from Redis
nations = pd.read_msgpack(redis_conn.get('nation'))

# Join dataframes
joined_df = (filtered_lineitems
    .merge(df_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    .merge(df_customers, left_on='C_CUSTKEY', right_on='C_CUSTKEY')
    .merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

# Filter only ASIA region (assuming it's regionkey 1, you will need to adjust if different)
asia_region = joined_df[joined_df['N_REGIONKEY'] == 1]

# Group by nation and calculate sum of revenue
result_df = asia_region.groupby('N_NAME')['REVENUE'].sum().reset_index()

# Order by revenue in descending order
result_df = result_df.sort_values(by='REVENUE', ascending=False)

# Write to CSV
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
