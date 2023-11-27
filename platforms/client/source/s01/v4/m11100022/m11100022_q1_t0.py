import pandas as pd
from direct_redis import DirectRedis

def calculate_aggregations(df):
    # Apply the query logic on the DataFrame
    result = (
        df[df['L_SHIPDATE'] <= '1998-09-02']
        .groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
        .agg(SUM_QTY=('L_QUANTITY', 'sum'),
             SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
             SUM_DISC_PRICE=(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()),
             SUM_CHARGE=(lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])).sum()),
             AVG_QTY=('L_QUANTITY', 'mean'),
             AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
             AVG_DISC=('L_DISCOUNT', 'mean'),
             COUNT_ORDER=('L_ORDERKEY', 'count'))
        .reset_index()
        .sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])
    )

    return result

# Connect to Redis
redis_host = 'redis'
redis_port = 6379
r = DirectRedis(host=redis_host, port=redis_port, db=0)

# Retrieve the lineitem table from Redis
lineitem_str = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_str)

# Calculate aggregations
aggregated_data = calculate_aggregations(lineitem_df)

# Output results to CSV
aggregated_data.to_csv('query_output.csv', index=False)
