import pandas as pd
from direct_redis import DirectRedis

# Function to convert data from Redis to a Pandas DataFrame
def get_redis_dataframe(redis_client, table_name):
    data = redis_client.get(table_name)
    if data is None:
        return pd.DataFrame()
    return pd.read_json(data, orient='records')

# Create a Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis
df_lineitem = get_redis_dataframe(redis_client, 'lineitem')

# Filter the DataFrame for dates before 1998-09-02
df_lineitem['L_SHIPDATE'] = pd.to_datetime(df_lineitem['L_SHIPDATE'])
filtered_df = df_lineitem.loc[df_lineitem['L_SHIPDATE'] < '1998-09-02']

# Calculate aggregates
grouped = filtered_df.groupby(['L_RETURNFLAG', 'L_LINESTATUS'])
summary = grouped.agg(
    Quantity_Total=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum'),
    Extended_Price_Total=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='sum'),
    Discounted_Price_Total=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT'])).sum()),
    Discounted_Price_Plus_Tax_Total=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc=lambda x: (x * (1 - filtered_df.loc[x.index, 'L_DISCOUNT']) * (1 + filtered_df.loc[x.index, 'L_TAX'])).sum()),
    Average_Quantity=pd.NamedAgg(column='L_QUANTITY', aggfunc='mean'),
    Average_Extended_Price=pd.NamedAgg(column='L_EXTENDEDPRICE', aggfunc='mean'),
    Average_Discount=pd.NamedAgg(column='L_DISCOUNT', aggfunc='mean'),
    Lineitem_Count=pd.NamedAgg(column='L_ORDERKEY', aggfunc='count')
).reset_index()

# Sort by return flag and line status
sorted_summary = summary.sort_values(by=['L_RETURNFLAG', 'L_LINESTATUS'])

# Save to a CSV file
sorted_summary.to_csv('query_output.csv', index=False)
