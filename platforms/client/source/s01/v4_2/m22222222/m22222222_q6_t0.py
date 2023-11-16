import redis
import pandas as pd

# Establish Connection
r = redis.Redis(
    host='redis',
    port=6379,
    db=0)

# Fetch Data
data = r.get('lineitem')
df = pd.DataFrame(data)

# Query
df['L_SHIPDATE'] = pd.to_datetime(df['L_SHIPDATE'])
mask = (df['L_SHIPDATE'] >= '1994-01-01') & (df['L_SHIPDATE'] < '1995-01-01') 
mask &= (df['L_DISCOUNT'] >= (.06 - 0.01)) & (df['L_DISCOUNT'] <= (.06 + 0.01))
mask &= df['L_QUANTITY'] < 24
df = df.loc[mask]

df['REVENUE'] = df['L_EXTENDEDPRICE'] * df['L_DISCOUNT']
revenue = df['REVENUE'].sum()

# Write to CSV
df.to_csv('query_output.csv', index=False)
