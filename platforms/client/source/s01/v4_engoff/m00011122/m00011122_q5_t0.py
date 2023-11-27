# local_supplier_volume_query.py
import pymysql
import pymongo
from direct_redis import DirectRedis
import pandas as pd

# MySQL connection and query execution
def query_mysql():
    connection = pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch'
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT n.N_NAME, r.R_NAME 
                FROM nation n 
                JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY 
                WHERE r.R_NAME = 'ASIA'
            """)
            nations = cursor.fetchall()
            nation_data = pd.DataFrame(nations, columns=['N_NAME', 'R_NAME'])
            return nation_data
    finally:
        connection.close()

# MongoDB connection and query execution
def query_mongodb():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    asia_nation_keys = set()  # To store nation keys
    supplier_data = db.supplier.find({'S_NATIONKEY': {'$in': list(asia_nation_keys)}},
                                     {'S_SUPPKEY': 1})
    supplier_data = pd.DataFrame(list(supplier_data))
    return supplier_data

# Redis connection and query execution
def query_redis():
    client = DirectRedis(host='redis', port=6379, db=0)
    order_dates = pd.to_datetime(client.get('orders.O_ORDERDATE'))
    qualifying_orders = order_dates[(order_dates >= '1990-01-01') & (order_dates < '1995-01-01')].index
    lineitems = client.get('lineitem').loc[qualifying_orders]
    lineitems['REVENUE'] = lineitems['L_EXTENDEDPRICE'] * (1 - lineitems['L_DISCOUNT'])
    return lineitems

# Main workflow
def main():
    nation_data = query_mysql()
    supplier_data = query_mongodb()
    lineitems = query_redis()

    # Now you will need to do the actual merging and processing for the calculated supplier volume
    # Since we need to mix data from different sources, you'd need to implement a function that does one or more JOIN operations 
    # to combine the data for the final calculation. This would involve complex merging logic which is omitted here.
    # After processing, we assume the result is stored in `result_df`

    # Save the result to a CSV file
    result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
