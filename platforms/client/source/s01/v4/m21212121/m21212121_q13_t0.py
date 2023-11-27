from pymongo import MongoClient
import pandas as pd
import direct_redis

def get_mongodb_data():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    customers = pd.DataFrame(list(db.customer.find()))
    return customers

def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    orders = r.get('orders')
    orders = pd.read_json(orders)
    return orders

def main():
    customers = get_mongodb_data()
    orders = get_redis_data()

    # Filtering orders that do not contain 'pending%deposits' in O_COMMENT
    orders = orders[~orders['O_COMMENT'].str.contains('pending%deposits', na=False)]

    # Merge customers and orders dataframes
    merged_data = pd.merge(customers, orders, how='left',
                           left_on='C_CUSTKEY', right_on='O_CUSTKEY')

    # Perform the GROUP BY and counting for the merged data
    grouped = merged_data.groupby('C_CUSTKEY', as_index=False).agg(C_COUNT=('O_ORDERKEY', 'count'))

    # Count the distribution of customers per C_COUNT
    custdist = grouped.groupby('C_COUNT', as_index=False).agg(CUSTDIST=('C_CUSTKEY', 'count'))

    # Sort by CUSTDIST DESC and C_COUNT DESC
    custdist.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

    # Writing to CSV file
    custdist.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
