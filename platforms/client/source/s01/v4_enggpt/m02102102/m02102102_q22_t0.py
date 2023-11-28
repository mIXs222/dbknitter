import pymysql
import pymongo
import pandas as pd
from pymongo import MongoClient

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Query MongoDB for customer data
mongo_customers = mongodb['customer'].find({}, {'_id': 0, 'C_CUSTKEY': 1, 'C_PHONE': 1, 'C_ACCTBAL': 1})
customer_df = pd.DataFrame(list(mongo_customers))
customer_df['CNTRYCODE'] = customer_df['C_PHONE'].str[:2]

# Calculate average account balance for customers with positive balances in specified country codes
avg_balances = customer_df[customer_df['C_ACCTBAL'] > 0].groupby('CNTRYCODE')['C_ACCTBAL'].mean().reset_index()
avg_balances.columns = ['CNTRYCODE', 'AVG_ACCTBAL']
avg_balances = avg_balances[avg_balances['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])]

# Merge customer data with average balances for selected country codes
customer_with_avg = pd.merge(customer_df, avg_balances, on='CNTRYCODE', how='inner')

# Filter customers with account balance greater than the average in their country code
filtered_customers = customer_with_avg[customer_with_avg['C_ACCTBAL'] > customer_with_avg['AVG_ACCTBAL']]

# Get the list of customer keys who placed orders from MySQL
with mysql_conn.cursor() as cursor:
    cursor.execute("SELECT DISTINCT O_CUSTKEY FROM orders")
    order_customer_keys = [row[0] for row in cursor.fetchall()]

# Exclude customers who have placed orders
final_customers = filtered_customers[~filtered_customers['C_CUSTKEY'].isin(order_customer_keys)]

# Aggregated results based on country codes
result = final_customers.groupby('CNTRYCODE').agg(NUMCUST=('C_CUSTKEY', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).reset_index()

# Sort results in ascending order based on country codes
result = result.sort_values(by='CNTRYCODE')

# Write results to CSV
result.to_csv('query_output.csv', index=False)

# Clean up
mysql_conn.close()
mongo_client.close()
