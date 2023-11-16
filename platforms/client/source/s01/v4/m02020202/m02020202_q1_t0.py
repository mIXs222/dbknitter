import pandas as pd
import direct_redis

# Define connection parameters for Redis
hostname = 'redis'
port = 6379
database_name = 0

# Connect to Redis using DirectRedis
r = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Read the DataFrame from Redis
lineitem_df = r.get('lineitem')

# Convert to Pandas DataFrame
lineitem_df = pd.DataFrame(
    lineitem_df,
    columns=[
        'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
        'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG',
        'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE',
        'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'
    ]
)

# Convert data types
lineitem_df['L_QUANTITY'] = lineitem_df['L_QUANTITY'].astype(float)
lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)
lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)
lineitem_df['L_TAX'] = lineitem_df['L_TAX'].astype(float)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Apply the SQL query logic
filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] <= pd.Timestamp('1998-09-02')]
result = (
    filtered_df
    .groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
    .agg(
        SUM_QTY=('L_QUANTITY', 'sum'),
        SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
        SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_df['L_DISCOUNT'])).sum()),
        SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: (x * (1 - filtered_df['L_DISCOUNT']) * (1 + filtered_df['L_TAX'])).sum()),
        AVG_QTY=('L_QUANTITY', 'mean'),
        AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
        AVG_DISC=('L_DISCOUNT', 'mean'),
        COUNT_ORDER=('L_ORDERKEY', 'count')
    )
    .reset_index()
    .sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])
)

# Write the result to a CSV file
result.to_csv('query_output.csv', index=False)

