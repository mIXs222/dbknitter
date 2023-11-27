import pandas as pd
import direct_redis

# Connect to Redis
hostname = 'redis'
port = 6379
r = direct_redis.DirectRedis(host=hostname, port=port)

# Retrieve the 'lineitem' table from Redis
lineitem_data = r.get('lineitem')

# Load the 'lineitem' data into a Pandas DataFrame
lineitem_df = pd.read_json(lineitem_data)

# Filter rows where L_SHIPDATE is before 1998-09-02
filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] < '1998-09-02']

# Calculate the required aggregates
aggregates = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    Total_Quantity=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
    Total_ExtendedPrice=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
    Total_Discounted_ExtendedPrice=pd.NamedAgg(
        column='L_EXTENDEDPRICE',
        aggfunc=lambda x: (x * (1 - filtered_df['L_DISCOUNT'])).sum()
    ),
    Total_Discounted_ExtendedPrice_Plus_Tax=pd.NamedAgg(
        column='L_EXTENDEDPRICE',
        aggfunc=lambda x: (x * (1 - filtered_df['L_DISCOUNT']) * (1 + filtered_df['L_TAX'])).sum()
    ),
    Avg_Quantity=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
    Avg_ExtendedPrice=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
    Avg_Discount=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
    Count_LineItems=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
)

# Resetting index to make L_RETURNFLAG and L_LINESTATUS as columns
aggregates = aggregates.reset_index()

# Sorting the aggregates by L_RETURNFLAG and L_LINESTATUS
sorted_aggregates = aggregates.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write to CSV
sorted_aggregates.to_csv('query_output.csv', index=False)
