import pandas as pd
import direct_redis

def query_redis():
    dr = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Use the get method to retrieve the tables as pandas dataframes
    df_customer = dr.get('customer')
    df_orders = dr.get('orders')

    # Perform equivalent operations in pandas for the SQL query provided
    avg_acctbal = df_customer[df_customer['C_ACCTBAL'] > 0.00]['C_ACCTBAL'].mean()
    cntrycode_filter = ['20', '40', '22', '30', '39', '42', '21']
    
    # Filtering customers based on phone and account balance
    filtered_customers = df_customer[
        (df_customer['C_PHONE'].str[:2].isin(cntrycode_filter)) &
        (df_customer['C_ACCTBAL'] > avg_acctbal)
    ].copy()

    # Finding customers without orders
    customers_without_orders = filtered_customers[
        ~filtered_customers['C_CUSTKEY'].isin(df_orders['O_CUSTKEY'])
    ]

    # Add CNTRYCODE to the filtered customers dataframe
    customers_without_orders['CNTRYCODE'] = customers_without_orders['C_PHONE'].str[:2]

    # Group by the country code, count customers and sum account balances
    result = customers_without_orders.groupby('CNTRYCODE').agg(
        NUMCUST=('C_CUSTKEY', 'count'),
        TOTACCTBAL=('C_ACCTBAL', 'sum')
    ).reset_index()

    # Write result to csv
    result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    query_redis()
