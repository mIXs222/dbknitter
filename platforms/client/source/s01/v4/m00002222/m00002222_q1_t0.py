import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis using the provided connection details
connection_info = {
    "host": "redis",
    "port": 6379,
    "db": 0
}
redis_client = DirectRedis(**connection_info)

# Get the lineitem DataFrame from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Executing the query using Pandas operations
query_result = (
    lineitem_df[lineitem_df["L_SHIPDATE"] <= '1998-09-02']
    .groupby(["L_RETURNFLAG", "L_LINESTATUS"])
    .agg(
        SUM_QTY=('L_QUANTITY', 'sum'),
        SUM_BASE_PRICE=('L_EXTENDEDPRICE', 'sum'),
        SUM_DISC_PRICE=('L_EXTENDEDPRICE', lambda x: (x * (1 - lineitem_df.loc[x.index, 'L_DISCOUNT'])).sum()),
        SUM_CHARGE=('L_EXTENDEDPRICE', lambda x: (x * (1 - lineitem_df.loc[x.index, 'L_DISCOUNT']) * (1 + lineitem_df.loc[x.index, 'L_TAX'])).sum()),
        AVG_QTY=('L_QUANTITY', 'mean'),
        AVG_PRICE=('L_EXTENDEDPRICE', 'mean'),
        AVG_DISC=('L_DISCOUNT', 'mean'),
        COUNT_ORDER=('L_ORDERKEY', 'count')
    )
    .sort_values(by=["L_RETURNFLAG", "L_LINESTATUS"])
    .reset_index()
)

# Save the query result to a CSV file
query_result.to_csv('query_output.csv', index=False)
