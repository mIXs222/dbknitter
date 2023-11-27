import pandas as pd
import direct_redis

# Connect to Redis
hostname = 'redis'
port = 6379
database_name = '0'
redis_connection = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Get lineitem table from Redis
lineitem_str = redis_connection.get('lineitem')
lineitem_data = pd.read_json(lineitem_str, orient='records')

# Filter out all lineitems with SHIPDATE earlier than 1998-09-02
lineitem_data['L_SHIPDATE'] = pd.to_datetime(lineitem_data['L_SHIPDATE'])
filtered_lineitems = lineitem_data[lineitem_data['L_SHIPDATE'] < '1998-09-02']

# Calculate aggregates
aggregations = {
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': ['mean'],
    'L_EXTENDEDPRICE_DISCOUNTED': ['sum'],
    'L_EXTENDEDPRICE_DISCOUNTED_PLUS_TAX': ['sum']
}

filtered_lineitems['L_EXTENDEDPRICE_DISCOUNTED'] = \
    filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])

filtered_lineitems['L_EXTENDEDPRICE_DISCOUNTED_PLUS_TAX'] = \
    filtered_lineitems['L_EXTENDEDPRICE_DISCOUNTED'] * (1 + filtered_lineitems['L_TAX'])

summary = filtered_lineitems.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(aggregations).reset_index()

# Rename columns to make them more readable
summary.columns = [
    'RETURNFLAG', 'LINESTATUS',
    'SUM_QUANTITY', 'AVG_QUANTITY',
    'SUM_EXTENDEDPRICE', 'AVG_EXTENDEDPRICE',
    'AVG_DISCOUNT',
    'SUM_DISC_PRICE',
    'SUM_DISC_PRICE_PLUS_TAX'
]

# Add count of lineitems
summary['COUNT_ORDER'] = filtered_lineitems.groupby(['L_RETURNFLAG', 'L_LINESTATUS'])['L_ORDERKEY'].count().values

# Sort by RETURNFLAG and LINESTATUS
summary.sort_values(by=['RETURNFLAG', 'LINESTATUS'], inplace=True)

# Write to CSV
summary.to_csv('query_output.csv', index=False)
