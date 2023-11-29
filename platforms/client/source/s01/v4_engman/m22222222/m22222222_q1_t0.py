import direct_redis
import pandas as pd
from datetime import datetime

# Connect to the Redis database
hostname = 'redis'
port = 6379
database_name = '0'

# Initialize DirectRedis connection
dr = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Read lineitem data as DataFrame
lineitem_df = dr.get('lineitem')

# Convert ship date to datetime and filter records shipped before 1998-09-02
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_df = lineitem_df[lineitem_df['L_SHIPDATE'] < datetime(1998, 9, 2)]

# Calculate aggregates
grouped = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
summary = grouped.agg(
    Total_Quantity=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
    Total_ExtendedPrice=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
    Total_DiscountedPrice=pd.NamedAgg(column=lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT'])).sum()),
    Total_DiscountedPrice_PlusTax=pd.NamedAgg(
        column=lambda x: (x['L_EXTENDEDPRICE'] * (1 - x['L_DISCOUNT']) * (1 + x['L_TAX'])).sum()
    ),
    Avg_Quantity=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
    Avg_ExtendedPrice=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
    Avg_Discount=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
    Count_Order=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
).reset_index()

# Sort by RETURNFLAG and LINESTATUS
summary_sorted = summary.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Write to CSV
summary_sorted.to_csv('query_output.csv', index=False)
