from pymongo import MongoClient
import pandas as pd

# Installing pandas using pip
os.system('python3 -m pip install pandas')

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Get tables as dataframes
df_customer = pd.DataFrame(list(db.customer.find()))
df_orders = pd.DataFrame(list(db.orders.find()))
df_lineitem = pd.DataFrame(list(db.lineitem.find()))

# Merge dataframes
df = pd.merge(df_customer, df_orders, how='inner', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
df = pd.merge(df, df_lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Create lineitem group
df_lineitem_group = df_lineitem.groupby(['L_ORDERKEY']).filter(lambda x: x['L_QUANTITY'].sum() > 300)

# Apply the filtered group to the dataframe
df = df[df['O_ORDERKEY'].isin(df_lineitem_group['L_ORDERKEY'].unique())]

# Group by necessary columns
df = df.groupby(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'], as_index=False)['L_QUANTITY'].sum()

# Sort df
df.sort_values(['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])
df.to_csv('query_output.csv', index=False)
