import pandas as pd
from redis import Redis

r = Redis(host='redis', port=6379, db=0)

# Load data from redis
customer = pd.DataFrame.from_dict(dict(r.get('customer')))
orders = pd.DataFrame.from_dict(dict(r.get('orders')))
lineitem = pd.DataFrame.from_dict(dict(r.get('lineitem')))

# Convert O_ORDERDATE to datetime format
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])

# Selecting L_ORDERKEY from lineitem where SUM(L_QUANTITY) > 300
filtered_lineitem = lineitem.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300)

# Merge tables
merged_data = pd.merge(customer, orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_data = pd.merge(merged_data, filtered_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Aggregate and sort data
result = merged_data.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])['L_QUANTITY'].sum().reset_index()
result = result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write result to csv
result.to_csv('query_output.csv', index=False)
