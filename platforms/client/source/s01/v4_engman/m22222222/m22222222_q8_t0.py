import pandas as pd
import direct_redis

# Establishing the connection to Redis
hostname = 'redis'
port = 6379
redis_connection = direct_redis.DirectRedis(host=hostname, port=port, db=0)

# Fetching the data from Redis
nation = pd.read_json(redis_connection.get('nation'))
region = pd.read_json(redis_connection.get('region'))
supplier = pd.read_json(redis_connection.get('supplier'))
lineitem = pd.read_json(redis_connection.get('lineitem'))
part = pd.read_json(redis_connection.get('part'))
orders = pd.read_json(redis_connection.get('orders'))

# Filter for year 1995 and 1996
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
orders_95_96 = orders[(orders['O_ORDERDATE'].dt.year == 1995) | 
                      (orders['O_ORDERDATE'].dt.year == 1996)]

# Filtering specific tables based on conditions
asia_region = region[region['R_NAME'] == 'ASIA']
india_nation = nation[(nation['N_NAME'] == 'INDIA') & 
                      (nation['N_REGIONKEY'].isin(asia_region['R_REGIONKEY']))]
small_plated_copper_parts = part[part['P_TYPE'] == 'SMALL PLATED COPPER']

# Combining data to calculate the market share
merged_df = (lineitem
             .merge(small_plated_copper_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
             .merge(orders_95_96, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
             .merge(supplier, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
             .merge(india_nation, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY'))

# Revenue calculation
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Market share calculation
market_share = (merged_df.groupby(merged_df['O_ORDERDATE'].dt.year)['REVENUE'].sum() 
                / merged_df.groupby(merged_df['O_ORDERDATE'].dt.year)['REVENUE'].transform('sum').unique())

# Formatting the final DataFrame
output = market_share.reset_index()
output.columns = ['ORDER_YEAR', 'MARKET_SHARE']

# Save the result to a CSV file
output.to_csv('query_output.csv', index=False)
