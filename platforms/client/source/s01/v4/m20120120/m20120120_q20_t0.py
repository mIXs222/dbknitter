import pandas as pd
import pymysql
import pymongo
import redis
from direct_redis import DirectRedis

def mysql_connection():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

def mongodb_connection():
    client = pymongo.MongoClient('mongodb', 27017)
    return client['tpch']

def redis_connection():
    return DirectRedis(host='redis', port=6379, db=0)

def fetch_mysql_data():
    mysql_conn = mysql_connection()
    try:
        with mysql_conn.cursor() as cursor:
            sql_partsupp = """
                SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY
                FROM partsupp
            """
            cursor.execute(sql_partsupp)
            partsupp_records = cursor.fetchall()
            df_partsupp = pd.DataFrame(partsupp_records, columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY'])

            sql_lineitem = """
                SELECT L_PARTKEY, L_SUPPKEY, L_QUANTITY
                FROM lineitem
                WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01'
            """
            cursor.execute(sql_lineitem)
            lineitem_records = cursor.fetchall()
            df_lineitem = pd.DataFrame(lineitem_records, columns=['L_PARTKEY', 'L_SUPPKEY', 'L_QUANTITY'])

    finally:
        mysql_conn.close()

    return df_partsupp, df_lineitem

def fetch_mongodb_data():
    mongodb_conn = mongodb_connection()
    part_records = list(mongodb_conn['part'].find({'P_NAME': {'$regex': '^forest'}}, {'P_PARTKEY': 1}))
    df_part = pd.DataFrame(part_records)
    return df_part

def fetch_redis_data():
    r = redis_connection()
    df_nation = pd.DataFrame(eval(r.get('nation')), columns=['N_NATIONKEY', 'N_NAME'])
    df_supplier = pd.DataFrame(eval(r.get('supplier')), columns=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY'])
    return df_nation, df_supplier

def execute_query():
    # Fetch data from different databases
    df_part = fetch_mongodb_data()
    df_nation, df_supplier = fetch_redis_data()
    df_partsupp, df_lineitem = fetch_mysql_data()

    # Process data to match the SQL logic
    half_qty = df_lineitem.groupby(['L_PARTKEY', 'L_SUPPKEY'])['L_QUANTITY'].sum() * 0.5
    df_partsupp = df_partsupp[df_partsupp['PS_PARTKEY'].isin(df_part['P_PARTKEY'])]
    df_partsupp = df_partsupp[df_partsupp.set_index(['PS_PARTKEY', 'PS_SUPPKEY'])['PS_AVAILQTY'] > half_qty]
    
    # Join with supplier and nation
    df_supplier = df_supplier[df_supplier['S_NATIONKEY'].isin(df_nation[df_nation['N_NAME'] == 'CANADA']['N_NATIONKEY'])]
    df_result = df_supplier[df_supplier['S_SUPPKEY'].isin(df_partsupp['PS_SUPPKEY'])]
    
    # Store the result in query_output.csv
    df_result = df_result.sort_values('S_NAME')
    df_result[['S_NAME', 'S_ADDRESS']].to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    execute_query()
