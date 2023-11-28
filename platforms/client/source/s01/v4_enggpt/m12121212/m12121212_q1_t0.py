import pandas as pd
import direct_redis

# Redis connection details
hostname = "redis"
port = 6379
database_name = 0

# Connecting to Redis
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Retrieving lineitem table from Redis
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Filtering data with shipping date on or before September 2, 1998
mask = pd.to_datetime(lineitem_df['L_SHIPDATE']) <= pd.to_datetime('1998-09-02')
filtered_df = lineitem_df.loc[mask]

# Performing aggregations
aggregations = {
    'L_QUANTITY': ['sum', 'mean'],
    'L_EXTENDEDPRICE': ['sum', 'mean'],
    'L_DISCOUNT': 'mean',
    'SUM_DISC_PRICE': lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum(),
    'SUM_CHARGE': lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])).sum(),
}

grouped_df = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    {
        'L_QUANTITY': ['sum', 'mean'],
        'L_EXTENDEDPRICE': ['sum', 'mean'],
        'L_DISCOUNT': 'mean',
        'L_EXTENDEDPRICE': lambda x: (x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT'])).sum(),
        'L_EXTENDEDPRICE': lambda x: (x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT']) * 
                                      (1 + filtered_df.loc[x.index, 'L_TAX'])).sum(),
    }
).reset_index()

grouped_df.columns = [
    'L_RETURNFLAG',
    'L_LINESTATUS',
    'SUM_QTY',
    'AVG_QTY',
    'SUM_BASE_PRICE',
    'AVG_PRICE',
    'AVG_DISC',
    'SUM_DISC_PRICE',
    'SUM_CHARGE',
]

grouped_df['COUNT_ORDER'] = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS'])['L_ORDERKEY'].transform('count')

# Sorting the results
sorted_df = grouped_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Saving the output to a CSV file
sorted_df.to_csv('query_output.csv', index=False)
