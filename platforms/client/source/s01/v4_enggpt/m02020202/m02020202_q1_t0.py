import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the Redis database
hostname = 'redis'
port = 6379
database_name = 0
r = DirectRedis(host=hostname, port=port, db=database_name)

# Fetch the lineitem data from Redis as a pandas DataFrame
lineitem_data = r.get('lineitem')

# Convert the retrieved data into a DataFrame
lineitem_df = pd.read_json(lineitem_data)

# Filter data for line items with a shipping date on or before September 2, 1998
lineitem_filtered_df = lineitem_df[
    pd.to_datetime(lineitem_df['L_SHIPDATE']) <= pd.Timestamp('1998-09-02')
]

# Define aggregation functions needed for grouped summary
aggregations = {
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': 'mean',
    'L_TAX': 'mean',
    'L_ORDERKEY': 'count'
}

# Perform the group by operation on return flag and line status
grouped_df = lineitem_filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(aggregations)

# Rename the columns to required names
grouped_df.columns = [
    'SUM_QTY', 'AVG_QTY', 'SUM_BASE_PRICE', 'AVG_PRICE',
    'AVG_DISC', 'SUM_CHARGE', 'COUNT_ORDER'
]

# Calculate the total discounted price
grouped_df['SUM_DISC_PRICE'] = lineitem_filtered_df.apply(
    lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']), axis=1
).groupby(
    [lineitem_filtered_df['L_RETURNFLAG'], lineitem_filtered_df['L_LINESTATUS']]
).transform('sum')

# Calculate the total charge
grouped_df['SUM_CHARGE'] = lineitem_filtered_df.apply(
    lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) * (1 + row['L_TAX']), axis=1
).groupby(
    [lineitem_filtered_df['L_RETURNFLAG'], lineitem_filtered_df['L_LINESTATUS']]
).transform('sum')

# Sort the results
sorted_df = grouped_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write the result to a CSV file
sorted_df.to_csv('query_output.csv')
