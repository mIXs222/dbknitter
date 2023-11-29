import pandas as pd
import pymongo
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
# Load data
region = pd.DataFrame(list(mongo_db.region.find()))
supplier = pd.DataFrame(list(mongo_db.supplier.find()))
customer = pd.DataFrame(list(mongo_db.customer.find()))
lineitem = pd.DataFrame(list(mongo_db.lineitem.find()))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
# Load data
nation = pd.read_json(redis_client.get('nation'))
part = pd.read_json(redis_client.get('part'))
orders = pd.read_json(redis_client.get('orders'))

# Join operations and calculations
asia_nations = nation[nation['N_REGIONKEY'] == region[region['R_NAME'] == 'ASIA']['R_REGIONKEY'].iloc[0]]
india_nations = asia_nations[asia_nations['N_NAME'] == 'INDIA']['N_NATIONKEY'].values

supplier_india = supplier[supplier['S_NATIONKEY'].isin(india_nations)]
supplier_customer = supplier_india.set_index('S_SUPPKEY').join(customer.set_index('C_CUSTKEY'))
supplier_customer_orders = supplier_customer.set_index('C_CUSTKEY').join(orders.set_index('O_CUSTKEY'))

small_plated_copper = part[part['P_TYPE'] == 'SMALL PLATED COPPER']
item_order_part = lineitem.set_index('L_PARTKEY').join(small_plated_copper.set_index('P_PARTKEY'))

final_join = supplier_customer_orders.join(item_order_part, lsuffix='_orders', rsuffix='_lineitems')

# Filter the orders which are made in 1995 or 1996
final_join = final_join[(final_join['O_ORDERDATE'].dt.year == 1995) | (final_join['O_ORDERDATE'].dt.year == 1996)]

# Calculate revenue
final_join['REVENUE'] = final_join['L_EXTENDEDPRICE'] * (1 - final_join['L_DISCOUNT'])

# Calculate total revenue per year
total_revenue = final_join.groupby(final_join['O_ORDERDATE'].dt.year)['REVENUE'].sum().rename('TOTAL_REVENUE')

# Calculate India's revenue per year
india_revenue = final_join[final_join['S_NATIONKEY'].isin(india_nations)].groupby(
    final_join['O_ORDERDATE'].dt.year)['REVENUE'].sum().rename('INDIA_REVENUE')

# Merge datasets and calculate the market share
result = pd.concat([india_revenue, total_revenue], axis=1)
result['MARKET_SHARE'] = result['INDIA_REVENUE'] / result['TOTAL_REVENUE']

# Drop unnecessary columns
output = result.drop(columns=['INDIA_REVENUE', 'TOTAL_REVENUE'])
output.reset_index(inplace=True)
output.columns = ['ORDER_YEAR', 'MARKET_SHARE']

# Output to csv
output.to_csv('query_output.csv', index=False)
