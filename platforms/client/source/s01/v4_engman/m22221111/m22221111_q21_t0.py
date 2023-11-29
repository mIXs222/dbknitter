# query.py

import pymongo
from pymongo import MongoClient
import pandas as pd
import direct_redis


def connect_mongodb():
    client = MongoClient("mongodb", 27017)
    return client['tpch']


def connect_redis():
    return direct_redis.DirectRedis(host="redis", port=6379, db=0)


def load_data_from_mongodb(db):
    orders = pd.DataFrame(list(db.orders.find({}, {'_id': 0})))
    lineitem = pd.DataFrame(list(db.lineitem.find({}, {'_id': 0})))

    return orders, lineitem


def load_data_from_redis(redis_conn):
    nation = pd.read_json(redis_conn.get('nation'))
    supplier = pd.read_json(redis_conn.get('supplier'))

    return nation, supplier


def query_execute(orders, lineitem, nation, supplier):
    # Filter for nation SAUDI ARABIA
    saudi_arabia_nation = nation[nation['N_NAME'] == 'SAUDI ARABIA']

    # Merge data sets
    merged_data = (
        supplier.merge(saudi_arabia_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
        .merge(lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
        .merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    )

    # Find orders with 'F' status and failed delivery dates
    failed_orders = (
        merged_data[merged_data['O_ORDERSTATUS'] == 'F']
    )

    # Identify multi-supplier orders
    multi_supplier_orders = (
        failed_orders.groupby('O_ORDERKEY').filter(lambda x: x['S_SUPPKEY'].nunique() > 1)
    )

    # Identify suppliers who were the only one failing to meet delivery date
    def supplier_failed_to_meet(row):
        return row['L_COMMITDATE'] < row['L_RECEIPTDATE'] and (row['L_COMMITDATE'] < row['O_ORDERDATE'])

    result = multi_supplier_orders[multi_supplier_orders.apply(supplier_failed_to_meet, axis=1)]
    result = result.groupby('S_NAME').size().reset_index(name='NUMWAIT')

    # Sort result
    result = result.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

    # Save to CSV file
    result.to_csv('query_output.csv', index=False)


def main():
    db = connect_mongodb()
    redis_conn = connect_redis()

    orders, lineitem = load_data_from_mongodb(db)
    nation, supplier = load_data_from_redis(redis_conn)

    query_execute(orders, lineitem, nation, supplier)


if __name__ == '__main__':
    main()
