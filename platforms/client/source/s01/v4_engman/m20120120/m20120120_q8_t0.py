# python code (query.py)

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']
customer_collection = mongo_db['customer']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Execute the query for MySQL (since lineitem is the only table in MySQL relevant to this query)
mysql_cursor.execute(
    "SELECT L_SHIPDATE, SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue "
    "FROM lineitem, region "
    "WHERE L_SHIPDATE LIKE '1995%%' OR L_SHIPDATE LIKE '1996%%' "
    "GROUP BY L_SHIPDATE"
)
lineitem_data = mysql_cursor.fetchall()
lineitem_df = pd.DataFrame(lineitem_data, columns=['L_SHIPDATE', 'revenue'])

# Query from MongoDB
part_docs = part_collection.find({'P_TYPE': 'SMALL PLATED COPPER'})

# Extract relevant data from Redis
nation_df = pd.read_json(redis_conn.get('nation'))
supplier_df = pd.read_json(redis_conn.get('supplier'))
orders_df = pd.read_json(redis_conn.get('orders'))

# Close MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Filter and process data
india_suppliers = supplier_df[supplier_df['S_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'INDIA']['N_NATIONKEY'].iloc[0]]
india_orders = orders_df[orders_df['O_ORDERKEY'].isin(Customer_df[Customer_df['C_NATIONKEY'].isin(india_suppliers['S_NATIONKEY'])]['C_CUSTKEY'])]
india_lineitem = LineItem_df[LineItem_df['L_ORDERKEY'].isin(india_orders['O_ORDERKEY'])]
india_revenue = india_lineitem.groupby([india_lineitem['L_SHIPDATE'].str[:4]]).apply(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum())

# Calculate market share
market_share = india_revenue / lineitem_df.groupby(lineitem_df['L_SHIPDATE'].str[:4])['revenue'].sum()

# Create the output DataFrame
output_df = pd.DataFrame({
    'order_year': market_share.index,
    'market_share': market_share.values
})

# Write to CSV file
output_df.to_csv('query_output.csv', index=False)
