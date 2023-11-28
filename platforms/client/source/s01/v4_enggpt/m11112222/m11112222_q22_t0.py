import pandas as pd
from direct_redis import DirectRedis

def main():
    # Connect to Redis
    r = DirectRedis(host='redis', port=6379, db=0)

    # Get dataframes
    customer_df = pd.read_json(r.get('customer'))
    orders_df = pd.read_json(r.get('orders'))

    # Data preparation
    customer_df['CNTRYCODE'] = customer_df['C_PHONE'].str[:2]
    avg_positive_balances = customer_df.loc[customer_df['C_ACCTBAL'] > 0].groupby('CNTRYCODE')['C_ACCTBAL'].mean()
    valid_cntrycodes = ['20', '40', '22', '30', '39', '42', '21']

    # Filter data using the criteria
    filtered_customers = customer_df[
        (customer_df['CNTRYCODE'].isin(valid_cntrycodes)) &
        (customer_df['C_ACCTBAL'] > customer_df['CNTRYCODE'].map(avg_positive_balances)) &
        (~customer_df['C_CUSTKEY'].isin(orders_df['O_CUSTKEY']))
    ]

    # Aggregate data
    stats = filtered_customers.groupby('CNTRYCODE').agg(
        NUMCUST=pd.NamedAgg(column='C_CUSTKEY', aggfunc='count'),
        TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', aggfunc='sum')
    ).reset_index()

    # Sort and save to csv
    stats.sort_values('CNTRYCODE').to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
