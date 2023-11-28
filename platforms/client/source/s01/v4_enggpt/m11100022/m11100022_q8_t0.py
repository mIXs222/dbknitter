import pymysql
import pymongo
import direct_redis
import pandas as pd

# Establish connections to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for customer and supplier tables
customer_query = "SELECT C_CUSTKEY, C_NATIONKEY FROM customer"
supplier_query = "SELECT S_SUPPKEY, S_NATIONKEY FROM supplier"

with mysql_conn.cursor() as cursor:
    cursor.execute(customer_query)
    customers = cursor.fetchall()
    cursor.execute(supplier_query)
    suppliers = cursor.fetchall()

# Convert query results to DataFrames
customers_df = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_NATIONKEY'])
suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NATIONKEY'])

# Retrieve data from MongoDB for part, nation, and region tables
parts_df = pd.DataFrame(list(mongo_db.part.find({'P_TYPE': 'SMALL PLATED COPPER'})))
nations_df = pd.DataFrame(list(mongo_db.nation.find()))
regions_df = pd.DataFrame(list(mongo_db.region.find({'R_NAME': 'ASIA'})))

# Get data from Redis for orders and lineitem tables
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Close the connections
mysql_conn.close()
mongo_client.close()
redis_client.close()

# Merge DataFrames
orders_df = orders_df[(orders_df['O_ORDERDATE'].dt.year.between(1995, 1996))]
lineitem_df = lineitem_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
lineitem_df['VOLUME'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
india_nationkey = nations_df[nations_df['N_NAME'] == 'INDIA']['N_NATIONKEY'].values[0]
asia_regionkey = regions_df['R_REGIONKEY'].values[0]

# Filter for ASIA region and INDIA customers/suppliers
customers_df = customers_df[customers_df['C_NATIONKEY'] == india_nationkey]
suppliers_df = suppliers_df[suppliers_df['S_NATIONKEY'] == india_nationkey]
nations_df = nations_df[nations_df['N_REGIONKEY'] == asia_regionkey]

# Filter lineitem for relevant parts and customers/suppliers from INDIA
lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(parts_df['P_PARTKEY'])]
lineitem_df = lineitem_df[(lineitem_df['L_SUPPKEY'].isin(suppliers_df['S_SUPPKEY'])) | 
                           (lineitem_df['L_ORDERKEY'].isin(customers_df['C_CUSTKEY']))]

# Calculate market share
total_volume_by_year = lineitem_df.groupby(lineitem_df['O_ORDERDATE'].dt.year)['VOLUME'].sum().rename('TOTAL_VOLUME')
india_volume_by_year = lineitem_df[lineitem_df['L_SUPPKEY'].isin(suppliers_df['S_SUPPKEY']) &
                                    (lineitem_df['O_ORDERDATE'].dt.year.between(1995, 1996))
                                   ].groupby(lineitem_df['O_ORDERDATE'].dt.year)['VOLUME'].sum().rename('INDIA_VOLUME')

market_share_by_year = india_volume_by_year.divide(total_volume_by_year)

# Output results
output_df = pd.DataFrame(market_share_by_year).reset_index().sort_values('O_ORDERDATE')
output_df.columns = ['YEAR', 'MARKET_SHARE']
output_df.to_csv('query_output.csv', index=False)

print('Analysis complete. Output written to query_output.csv.')

