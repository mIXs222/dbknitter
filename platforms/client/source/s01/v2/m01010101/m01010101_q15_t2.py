import pandas as pd
from pymongo import MongoClient

# Create Connection
client = MongoClient("mongodb://localhost:27017/")
mydb = client["tpch"]

supplier = pd.DataFrame(list(mydb.supplier.find({},{ "_id": 0 })))
lineitem = pd.DataFrame(list(mydb.lineitem.find({},{ "_id": 0 })))

# Filter lineitem data based on date and create a pandas data frame as per requirement
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
start_date = '1996-01-01'
end_date = '1994-04-01'
mask = (lineitem['L_SHIPDATE'] > start_date) & (lineitem['L_SHIPDATE'] <= end_date)

filtered_lineitem = lineitem.loc[mask]

filtered_lineitem['TOTAL_REVENUE'] = filtered_lineitem['L_EXTENDEDPRICE'] * (1 - filtered_lineitem['L_DISCOUNT'])
revenue_table = filtered_lineitem.groupby('L_SUPPKEY').agg({'TOTAL_REVENUE': ['sum']})
revenue_table.columns = ['TOTAL_REVENUE']
revenue_table = revenue_table.reset_index()

# Get the maximum value of TOTAL_REVENUE
max_revenue = revenue_table['TOTAL_REVENUE'].max()

result_df = pd.merge(supplier, revenue_table,  how='inner', left_on='S_SUPPKEY', right_on = 'L_SUPPKEY')

result_df = result_df[result_df['TOTAL_REVENUE'] == max_revenue]
result_df = result_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]
result_df.to_csv('query_output.csv', index=False)
