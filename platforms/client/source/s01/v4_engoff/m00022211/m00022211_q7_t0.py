import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Retrieve `nation` from MySQL
nation_query = "SELECT * FROM nation WHERE N_NAME = 'INDIA' OR N_NAME = 'JAPAN';"
mysql_cursor.execute(nation_query)
nation_records = mysql_cursor.fetchall()
# Convert to DataFrame for easier merging
nation_df = pd.DataFrame(nation_records, columns=['N_NATIONKEY', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT'])

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
# Retrieve `orders` and `lineitem` from MongoDB
orders = list(mongo_db.orders.find({'$and': [{'O_ORDERDATE': {'$gte': '1995-01-01', '$lt': '1997-01-01'}}, {'O_ORDERSTATUS': {'$in': ['F', 'O', 'P']}}]}))
lineitem = list(mongo_db.lineitem.find())
# Convert to DataFrame
orders_df = pd.DataFrame(orders).rename(columns={'_id': 'id'})
lineitem_df = pd.DataFrame(lineitem).rename(columns={'_id': 'id'})

# Merge lineitem with orders
lineitem_orders_df = pd.merge(lineitem_df, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)
# Retrieve `supplier` and `customer` from Redis
suppliers_data = redis_conn.get('supplier')
suppliers_df = pd.read_json(suppliers_data)
customers_data = redis_conn.get('customer')
customers_df = pd.read_json(customers_data)

# Close connections to save resources
mysql_cursor.close()
mysql_conn.close()

# Join nation with suppliers and customers
suppliers_nation_df = pd.merge(suppliers_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
customers_nation_df = pd.merge(customers_df, nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Find lineitems with suppliers and customers from different nations
lineitem_cross_nation_df = lineitem_orders_df.merge(suppliers_nation_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY') \
                                             .merge(customers_nation_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY') \
                                             .query("N_NAME_x != N_NAME_y")

# Calculate gross discounted revenues
lineitem_cross_nation_df['revenue'] = lineitem_cross_nation_df['L_EXTENDEDPRICE'] * (1 - lineitem_cross_nation_df['L_DISCOUNT'])

# Extract year from orders
lineitem_cross_nation_df['year'] = pd.to_datetime(lineitem_cross_nation_df['O_ORDERDATE']).dt.year

# Selecting required columns and rename for clarity
output_df = lineitem_cross_nation_df[['N_NAME_x', 'N_NAME_y', 'year', 'revenue']] \
    .rename(columns={'N_NAME_x': 'supplier_nation', 'N_NAME_y': 'customer_nation'}) \
    .groupby(['supplier_nation', 'customer_nation', 'year']).sum().reset_index()

# Order by Supplier nation, Customer nation, and year
output_df_sorted = output_df.sort_values(by=['supplier_nation', 'customer_nation', 'year'])

# Write the result to a CSV file
output_df_sorted.to_csv('query_output.csv', index=False)
