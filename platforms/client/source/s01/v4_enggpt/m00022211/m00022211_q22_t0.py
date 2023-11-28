import pandas as pd
import pymongo
from direct_redis import DirectRedis

# ---- MONGODB CONNECTION ---- #
def get_mongodb_orders():
    client = pymongo.MongoClient("mongodb", 27017)
    db = client['tpch']
    orders_collection = db['orders']
    orders_df = pd.DataFrame(list(orders_collection.find()))
    client.close()
    return orders_df

# ---- REDIS CONNECTION ---- #
def get_redis_customers():
    dredis = DirectRedis(host='redis', port=6379, db=0)
    customer_df = pd.read_json(dredis.get('customer'))
    return customer_df

# ---- PROCESSING DATA ---- #
def process_data():
    # Retrieve data
    orders_df = get_mongodb_orders()
    customer_df = get_redis_customers()

    # Extract country codes
    customer_df['CNTRYCODE'] = customer_df['C_PHONE'].str[:2]

    # Calculate average account balance for given country codes
    country_codes = ['20', '40', '22', '30', '39', '42', '21']
    avg_acct_bal = customer_df[
        customer_df['C_ACCTBAL'] > 0 & customer_df['CNTRYCODE'].isin(country_codes)
    ].groupby('CNTRYCODE')['C_ACCTBAL'].mean().to_dict()

    # Filter customers based on account balance criteria
    def filter_customers(row):
        return row['C_ACCTBAL'] > avg_acct_bal.get(row['CNTRYCODE'], 0)
    valid_customers = customer_df[customer_df.apply(filter_customers, axis=1)]

    # Exclude customers with existing orders
    orders_cust_keys = orders_df['O_CUSTKEY'].unique()
    valid_customers = valid_customers[~valid_customers['C_CUSTKEY'].isin(orders_cust_keys)]

    # Aggregate data
    custsale = valid_customers.groupby('CNTRYCODE').agg(
        NUMCUST=pd.NamedAgg(column='C_CUSTKEY', func='count'),
        TOTACCTBAL=pd.NamedAgg(column='C_ACCTBAL', func='sum')
    ).reset_index().sort_values('CNTRYCODE')

    # Write to CSV
    custsale.to_csv('query_output.csv', index=False)

# ---- MAIN EXECUTION ---- #
if __name__ == '__main__':
    process_data()
