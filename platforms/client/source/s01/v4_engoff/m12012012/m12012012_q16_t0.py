# query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

def get_mysql_connection():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

def get_mongo_connection():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

def get_redis_connection():
    return DirectRedis(host='redis', port=6379, db=0)

def query_mysql():
    connection = get_mysql_connection()
    query = '''
    SELECT P_PARTKEY, P_NAME FROM part
    WHERE P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    '''
    with connection.cursor() as cursor:
        cursor.execute(query)
        parts_df = pd.DataFrame(cursor.fetchall(), columns=['P_PARTKEY', 'P_NAME'])
    connection.close()
    return parts_df

def query_mongodb():
    db = get_mongo_connection()
    supplier_collection = db['supplier']
    suppliers = list(supplier_collection.find(
        {"$and": [
            {"S_COMMENT": {"$not": {"$regex": "Customer.*Complaints"}}}
        ]}
    ))
    suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NAME'])
    return suppliers_df

def query_redis(parts_df):
    r = get_redis_connection()
    partsupp_keys = parts_df['P_PARTKEY'].astype(str).tolist()
    part_suppliers_list = [eval(r.get(f'partsupp:{key}')) for key in partsupp_keys if r.get(f'partsupp:{key}')]
    partsupp_df = pd.DataFrame(part_suppliers_list, columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])
    return partsupp_df

def main():
    parts_df = query_mysql()
    suppliers_df = query_mongodb()
    partsupp_df = query_redis(parts_df)
    
    # Merge the data frames based on the supplier key
    result_df = pd.merge(partsupp_df, suppliers_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
    
    # Group by the particular attributes mentioned and count the suppliers
    final_result_df = result_df.groupby(['P_NAME']).size().reset_index(name='SUPPLIER_COUNT')
    
    # Sort as required in the query
    final_result_df = final_result_df.sort_values(by=['SUPPLIER_COUNT', 'P_NAME'], ascending=[False, True])
    
    # Write to CSV
    final_result_df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
