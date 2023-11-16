import pandas as pd
import direct_redis

def query_redis():
    # Connection to Redis
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

    # Get DataFrames from Redis
    df_customer_raw = pd.read_json(r.get('customer'), orient='records')
    df_orders_raw = pd.read_json(r.get('orders'), orient='records')

    # Data transformation as per SQL query
    df_customer = df_customer_raw.copy()
    df_customer['CNTRYCODE'] = df_customer['C_PHONE'].str[:2]
    df_orders = df_orders_raw.copy()

    # Filtering customers
    countries = ('20', '40', '22', '30', '39', '42', '21')
    df_customer = df_customer[df_customer['CNTRYCODE'].isin(countries)]

    # Calculate average account balance
    avg_acctbal = df_customer[df_customer['C_ACCTBAL'] > 0]['C_ACCTBAL'].mean()
    df_customer = df_customer[df_customer['C_ACCTBAL'] > avg_acctbal]

    # Ensure customers have no orders
    df_customer = df_customer[~df_customer['C_CUSTKEY'].isin(df_orders['O_CUSTKEY'])]

    # Aggregate by country code
    result = df_customer.groupby('CNTRYCODE').agg(NUMCUST=('C_CUSTKEY', 'count'), TOTACCTBAL=('C_ACCTBAL', 'sum'))

    # Write to CSV
    result.reset_index().to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    query_redis()
