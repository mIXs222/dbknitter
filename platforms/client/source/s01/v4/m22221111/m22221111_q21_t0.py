import pymongo
import pandas as pd
from pandas import DataFrame
import direct_redis

def connect_to_mongodb(host, port, db_name):
    client = pymongo.MongoClient(host, port)
    return client[db_name]

def connect_to_redis(host, port, db_name):
    return direct_redis.DirectRedis(host=host, port=port, db=db_name)

def query_mongodb(mongo_db, cols_orders, cols_lineitem):
    orders = DataFrame(list(mongo_db.orders.find({}, cols_orders)))
    lineitem = DataFrame(list(mongo_db.lineitem.find({}, cols_lineitem)))
    return orders, lineitem

def query_redis(redis_db):
    nation = DataFrame(eval(redis_db.get('nation')))
    supplier = DataFrame(eval(redis_db.get('supplier')))
    return nation, supplier

def perform_query(orders, lineitem, supplier, nation):
    merged = lineitem.merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    merged = merged[merged['O_ORDERSTATUS'] == 'F']
    merged = merged[merged['L_RECEIPTDATE'] > merged['L_COMMITDATE']]

    suppliers = supplier[supplier['S_NATIONKEY'].isin(nation[nation['N_NAME'] == 'SAUDI ARABIA']['N_NATIONKEY'])]
    result = merged.merge(suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

    temp = lineitem.merge(result[['L_ORDERKEY', 'L_SUPPKEY']], on='L_ORDERKEY')
    temp = temp[temp['L_SUPPKEY_x'] != temp['L_SUPPKEY_y']]
    lineitem_with_different_suppliers = temp['L_ORDERKEY'].unique()

    result = result[result['L_ORDERKEY'].isin(lineitem_with_different_suppliers)]
    
    result_with_receipt_late = result[result['L_RECEIPTDATE'] > result['L_COMMITDATE']]
    orders_with_late_receipt = result_with_receipt_late['L_ORDERKEY'].unique()

    final_result = result[~result['L_ORDERKEY'].isin(orders_with_late_receipt)]
    final_result = final_result.groupby('S_NAME').size().reset_index(name='NUMWAIT')
    final_result = final_result.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

    return final_result

def main():
    mongo_db = connect_to_mongodb('mongodb', 27017, 'tpch')
    redis_db = connect_to_redis('redis', 6379, 0)

    cols_orders = {'_id': 0, 'O_ORDERKEY': 1, 'O_ORDERSTATUS': 1}
    cols_lineitem = {'_id': 0, 'L_ORDERKEY': 1, 'L_SUPPKEY': 1, 'L_RECEIPTDATE': 1, 'L_COMMITDATE': 1}

    orders, lineitem = query_mongodb(mongo_db, cols_orders, cols_lineitem)
    nation, supplier = query_redis(redis_db)

    final_result = perform_query(orders, lineitem, supplier, nation)
    final_result.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
