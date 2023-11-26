import pymongo
import pandas as pd

# Connect to MongoDB instance
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']

# Fetch data from collections and create DataFrames
customers = pd.DataFrame(list(db['customer'].find()))
orders = pd.DataFrame(list(db['orders'].find()))

# Process customer accounts with positive balance
positive_balances = customers[customers['C_ACCTBAL'] > 0.00]
avg_positive_balance = positive_balances['C_ACCTBAL'].mean()

# Filter customers based on the conditions and prepare subquery result
subquery_result = customers[
    (customers['C_PHONE'].str[:2].isin(['20', '40', '22', '30', '39', '42', '21'])) &
    (customers['C_ACCTBAL'] > avg_positive_balance)
].copy()

# Exclude customers who have orders
subquery_result = subquery_result[~subquery_result['C_CUSTKEY'].isin(orders['O_CUSTKEY'])]

# Add CNTRYCODE column
subquery_result['CNTRYCODE'] = subquery_result['C_PHONE'].str[:2]

# Group by CNTRYCODE and perform aggregation
output = subquery_result.groupby('CNTRYCODE').agg(
    NUMCUST=('C_CUSTKEY', 'count'),
    TOTACCTBAL=('C_ACCTBAL', 'sum')
).reset_index()

# Order by CNTRYCODE
output.sort_values(by='CNTRYCODE', inplace=True)

# Write to CSV
output.to_csv('query_output.csv', index=False)
