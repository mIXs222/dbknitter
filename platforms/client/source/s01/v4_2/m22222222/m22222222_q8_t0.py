import pandas as pd
import redis

# Connect to Redis
redis_db = redis.StrictRedis(host="redis", port=6379, db=0)

# Load the data from Redis
nation = pd.DataFrame.from_records(redis_db.get("nation"))
region = pd.DataFrame.from_records(redis_db.get("region"))
part = pd.DataFrame.from_records(redis_db.get("part"))
supplier = pd.DataFrame.from_records(redis_db.get("supplier"))
customer = pd.DataFrame.from_records(redis_db.get("customer"))
orders = pd.DataFrame.from_records(redis_db.get("orders"))
lineitem = pd.DataFrame.from_records(redis_db.get("lineitem"))

# Preprocess date
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
orders['O_YEAR'] = orders['O_ORDERDATE'].dt.year

# Filter data
orders = orders[(orders['O_ORDERDATE'] >= '1995-01-01') & (orders['O_ORDERDATE'] <= '1996-12-31')]
region = region[region['R_NAME'] == 'ASIA']
part = part[part['P_TYPE'] == 'SMALL PLATED COPPER']

# Merge data
df = pd.merge(orders, customer, on='C_CUSTKEY')
df = pd.merge(df, lineitem, on='L_ORDERKEY')
df = pd.merge(df, supplier, on='S_SUPPKEY')
df = pd.merge(df, part, on='P_PARTKEY')
df = pd.merge(df, region, left_on='C_NATIONKEY', right_on='R_REGIONKEY')
df = pd.merge(df, nation, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Calculate volume
df['VOLUME'] = df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])
df['INDIA'] = df['N_NAME'] == 'INDIA'
df['INDIA_VOLUME'] = df['INDIA'] * df['VOLUME']

results = df.groupby('O_YEAR').apply(lambda df: df['INDIA_VOLUME'].sum() / df['VOLUME'].sum()).reset_index()
results.columns = ['O_YEAR', 'MKT_SHARE']

# Save result
results.to_csv('query_output.csv', index=False)
