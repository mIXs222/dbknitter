import pymysql
import pymongo
import pandas as pd
import csv

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Fetch customers from MySQL database
mysql_cursor.execute("""
    SELECT C_CUSTKEY, C_NAME, C_ADDRESS, SUBSTR(C_PHONE, 1, 2) as CNTRYCODE, C_ACCTBAL
    FROM customer
    WHERE SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21') AND C_ACCTBAL > 0;
""")
customers = mysql_cursor.fetchall()
df_customers = pd.DataFrame(list(customers), columns=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS', 'CNTRYCODE', 'C_ACCTBAL'])

# Calculate average account balance for specified country codes
avg_acct_balance = df_customers.groupby('CNTRYCODE')['C_ACCTBAL'].mean().to_dict()

# Filter customers with account balances greater than the average
df_customers['Above_Avg'] = df_customers.apply(lambda x: x['C_ACCTBAL'] > avg_acct_balance[x['CNTRYCODE']], axis=1)
df_customers = df_customers[df_customers['Above_Avg']]

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
mongo_orders = mongo_db['orders']

# Exclude customers who have placed orders
cust_with_orders = mongo_orders.distinct('O_CUSTKEY')
df_customers = df_customers[~df_customers.C_CUSTKEY.isin(cust_with_orders)]

# Aggregate based on country codes
result = df_customers.groupby('CNTRYCODE').agg(NUMCUST=('C_CUSTKEY', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).reset_index()

# Order results in ascending order by country codes
result.sort_values(by=['CNTRYCODE'], inplace=True)

# Write results to CSV file
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
