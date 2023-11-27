import pandas as pd
import direct_redis

# Connect to the Redis database
hostname = 'redis'
port = 6379
database_name = '0'
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Read lineitem table from Redis
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Convert columns to appropriate data types
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_QUANTITY'] = lineitem_df['L_QUANTITY'].astype(float)
lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)
lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)
lineitem_df['L_TAX'] = lineitem_df['L_TAX'].astype(float)

# Filter items shipped before 1998-09-02
filtered_lineitem_df = lineitem_df[lineitem_df['L_SHIPDATE'] < pd.Timestamp('1998-09-02')]

# Perform aggregations
aggregation = {
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': 'mean',
    'L_EXTENDEDPRICE_DISCOUNT': 'sum',
    'L_EXTENDEDPRICE_PLUS_TAX': 'sum'
}

# Calculate discounted extended price and discounted extended price plus tax
filtered_lineitem_df['L_EXTENDEDPRICE_DISCOUNT'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])
filtered_lineitem_df['L_EXTENDEDPRICE_PLUS_TAX'] = filtered_lineitem_df['L_EXTENDEDPRICE_DISCOUNT'] * (1 + filtered_lineitem_df['L_TAX'])

grouped_lineitem_df = filtered_lineitem_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(aggregation).reset_index()

# Rename columns
grouped_lineitem_df.columns = [
    'L_RETURNFLAG', 'L_LINESTATUS',
    'SUM_QTY', 'AVG_QTY',
    'SUM_BASE_PRICE', 'AVG_PRICE',
    'AVG_DISCOUNT',
    'SUM_DISC_PRICE',
    'SUM_CHARGE'
]

# Add the count of lineitems
grouped_lineitem_df['COUNT_ORDER'] = filtered_lineitem_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS'])['L_ORDERKEY'].transform('count')

# Sort by RETURNFLAG and LINESTATUS
grouped_lineitem_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'], ascending=True, inplace=True)

# Write to CSV file
grouped_lineitem_df.to_csv('query_output.csv', index=False)
