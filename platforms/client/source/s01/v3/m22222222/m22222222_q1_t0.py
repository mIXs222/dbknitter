from direct_redis import DirectRedis
import pandas as pd
import csv

# Assuming the data is saved in form of list of dictionaries
# each representing a row in the table

# Connecting to Redis
client = DirectRedis(host='redis', port=6379, db=0)

# Retrieving data
data = client.get('lineitem')

# Converting data to DataFrame
df = pd.DataFrame(data)

# Date filtering
df['L_SHIPDATE'] = pd.to_datetime(df['L_SHIPDATE'])
df = df[df['L_SHIPDATE'] <= '1998-09-02']

# Aggregation
grouped_df = df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg(
    SUM_QTY=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
    SUM_BASE_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
    SUM_DISC_PRICE=pd.NamedAgg(column=(df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT'])), aggfunc='sum'),
    SUM_CHARGE=pd.NamedAgg(column=(df['L_EXTENDEDPRICE'] * (1 - df['L_DISCOUNT']) * (1 + df['L_TAX'])), aggfunc='sum'),
    AVG_QTY=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
    AVG_PRICE=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
    AVG_DISC=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
    COUNT_ORDER=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
).reset_index()

# Sorting
grouped_df = grouped_df.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Writing to csv
grouped_df.to_csv('query_output.csv', index=False)
