# query.py

import pymysql
import pandas as pd
import direct_redis

def main():
    # Connect to MySQL database
    mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
    
    # Fetch customers with positive account balance from MySQL
    positive_balance_cust_query = """
    SELECT C_CUSTKEY, C_PHONE, C_ACCTBAL
    FROM customer
    WHERE C_ACCTBAL > 0
    """
    customer_positive_balance_df = pd.read_sql(positive_balance_cust_query, mysql_conn)

    # Calculate the average account balance for customers in specific country codes
    customer_positive_balance_df['CNTRYCODE'] = customer_positive_balance_df['C_PHONE'].str[:2]
    avg_acct_bal_df = customer_positive_balance_df[customer_positive_balance_df['CNTRYCODE'].isin(['20', '40', '22', '30', '39', '42', '21'])].groupby('CNTRYCODE')['C_ACCTBAL'].mean().reset_index()
    avg_acct_bal_df.columns = ['CNTRYCODE', 'AVG_ACCTBAL']
    
    # Connect to Redis database
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    customer_df = pd.DataFrame(eval(r.get('customer')))

    # Merge with average account balance data
    qualified_customers_df = customer_df.merge(avg_acct_bal_df, left_on='C_PHONE', right_on='CNTRYCODE', how='inner')

    # Filter customers with account balance greater than the average
    qualified_customers_df = qualified_customers_df[qualified_customers_df['C_ACCTBAL'] > qualified_customers_df['AVG_ACCTBAL']]
    
    # Check for customers that have not placed any orders (using the NOT EXISTS condition)
    orders_df = pd.DataFrame(eval(r.get('orders')))
    qualified_customers_df = qualified_customers_df[~qualified_customers_df['C_CUSTKEY'].isin(orders_df['O_CUSTKEY'])][['CNTRYCODE', 'C_CUSTKEY', 'C_ACCTBAL']]
    
    # Group by country code and calculate the required statistics
    results = qualified_customers_df.groupby('CNTRYCODE').agg(NUMCUST=('C_CUSTKEY', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum')).reset_index()
    
    # Sort the results by country code
    results = results.sort_values('CNTRYCODE')
    
    # Write the results to a CSV file
    results.to_csv('query_output.csv', index=False)

# Close the database connection and run the main function
if __name__ == '__main__':
    main()
