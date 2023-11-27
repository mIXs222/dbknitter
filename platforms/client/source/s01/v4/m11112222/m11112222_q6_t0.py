import pandas as pd
from direct_redis import DirectRedis

# Connection details
hostname = 'redis'
port = 6379
dbname = 0

# Initialize a connection to Redis
client = DirectRedis(host=hostname, port=port, db=dbname)

# Retrieve the data from Redis
lineitem = pd.DataFrame(client.get('lineitem'))

# Convert relevant fields to appropriate types
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
lineitem['L_EXTENDEDPRICE'] = lineitem['L_EXTENDEDPRICE'].astype(float)
lineitem['L_DISCOUNT'] = lineitem['L_DISCOUNT'].astype(float)
lineitem['L_QUANTITY'] = lineitem['L_QUANTITY'].astype(int)

# Apply the filters as in the SQL query
filtered_data = lineitem[
    (lineitem['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem['L_SHIPDATE'] < '1995-01-01') &
    (lineitem['L_DISCOUNT'] >= 0.06 - 0.01) &
    (lineitem['L_DISCOUNT'] <= 0.06 + 0.01) &
    (lineitem['L_QUANTITY'] < 24)
]

# Calculate the revenue
filtered_data['REVENUE'] = filtered_data['L_EXTENDEDPRICE'] * filtered_data['L_DISCOUNT']

# Write the results to a CSV file
result = filtered_data[['REVENUE']].sum(axis=0)
result.to_csv('query_output.csv', header=True)
