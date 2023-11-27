import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis database
hostname = "redis"
port = 6379
database_name = "0"
r = DirectRedis(host=hostname, port=port, db=database_name)

# Retrieve lineitem table from Redis
lineitem_data = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Perform the query on the DataFrame
result = (
    lineitem_df[lineitem_df['L_SHIPDATE'] <= '1998-09-02']
    .groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
    .agg(
        SUM_QTY=('L_QUANTITY', 'sum'),
        SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
        SUM_DISC_PRICE=(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()),
        SUM_CHARGE=(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])).sum()),
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
