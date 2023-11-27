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

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get the nation data from Redis
nation_data = redis_client.get('nation').decode('utf-8')
nation_df = pd.read_json(nation_data)

# Filter nations (INDIA and JAPAN)
india_nationkey = nation_df.loc[nation_df['N_NAME'] == 'INDIA', 'N_NATIONKEY'].iloc[0]
japan_nationkey = nation_df.loc[nation_df['N_NAME'] == 'JAPAN', 'N_NATIONKEY'].iloc[0]

# Get the customer and supplier data from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT * FROM customer WHERE C_NATIONKEY IN (%s, %s)", (india_nationkey, japan_nationkey))
    customer_data = cursor.fetchall()
    cursor.execute("SELECT * FROM supplier WHERE S_NATIONKEY IN (%s, %s)", (india_nationkey, japan_nationkey))
    supplier_data = cursor.fetchall()

# Convert MySQL data to pandas DataFrame
customer_df = pd.DataFrame(customer_data, columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])
supplier_df = pd.DataFrame(supplier_data, columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

# Get the orders and lineitem data from MongoDB
orders_col = mongo_db['orders']
lineitem_col = mongo_db['lineitem']
orders_df = pd.DataFrame(list(orders_col.find()))
lineitem_df = pd.DataFrame(list(lineitem_col.find()))

# Close the MySQL connection
mysql_conn.close()

# Filter orders by date
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders_df = orders_df[(orders_df['O_ORDERDATE'].dt.year == 1995) | (orders_df['O_ORDERDATE'].dt.year == 1996)]

# Join operations
result_df = lineitem_df.merge(filtered_orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
result_df = result_df.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')
result_df = result_df.merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Calculate revenue
result_df['REVENUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])

# Filter required relations between countries
india_to_japan = (result_df['S_NATIONKEY'] == india_nationkey) & (result_df['C_NATIONKEY'] == japan_nationkey)
japan_to_india = (result_df['S_NATIONKEY'] == japan_nationkey) & (result_df['C_NATIONKEY'] == india_nationkey)
result_df = result_df[india_to_japan | japan_to_india]

# Final projection and sorting
output_df = result_df[['S_NATIONKEY', 'C_NATIONKEY', 'O_ORDERDATE', 'REVENUE']]
output_df = output_df.rename(columns={'S_NATIONKEY': 'SUPPLIER_NATION', 'C_NATIONKEY': 'CUSTOMER_NATION', 'O_ORDERDATE': 'YEAR'})
output_df['YEAR'] = output_df['YEAR'].dt.year
output_df = output_df.groupby(['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR']).sum().reset_index()
output_df = output_df.sort_values(by=['SUPPLIER_NATION', 'CUSTOMER_NATION', 'YEAR'])

# Write result to file
output_df.to_csv('query_output.csv', index=False)
