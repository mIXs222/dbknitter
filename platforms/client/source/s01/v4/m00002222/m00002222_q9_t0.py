import pandas as pd
import pymysql
from direct_redis import DirectRedis

def query_mysql():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    try:
        with connection.cursor() as cursor:
            mysql_query = """
            SELECT N_NAME, S_SUPPKEY, P_PARTKEY 
            FROM nation, supplier, part 
            WHERE S_NATIONKEY = N_NATIONKEY 
              AND P_NAME LIKE '%dim%'
            """
            cursor.execute(mysql_query)
            return cursor.fetchall()
    finally:
        connection.close()

def query_redis(redis_client):
    partsupp_df = pd.DataFrame(redis_client.get('partsupp'))
    orders_df = pd.DataFrame(redis_client.get('orders'))
    lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

    redis_df = lineitem_df.merge(partsupp_df, how='inner', on=['PS_SUPPKEY', 'PS_PARTKEY']) \
                          .merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    return redis_df

def calculate_profit(mysql_data, redis_data):
    # Convert fetched data to DataFrame
    mysql_df = pd.DataFrame(mysql_data, columns=['N_NAME', 'S_SUPPKEY', 'P_PARTKEY'])

    # Merge MySQL and Redis data
    merged_df = redis_data.merge(mysql_df, how='inner', left_on=['L_SUPPKEY', 'L_PARTKEY'], right_on=['S_SUPPKEY', 'P_PARTKEY'])

    # Calculate O_YEAR
    merged_df['O_YEAR'] = pd.to_datetime(merged_df['O_ORDERDATE']).dt.year

    # Calculate profit (AMOUNT)
    merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - \
                          merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']

    # Group by NATION and O_YEAR
    result = merged_df.groupby(['N_NAME', 'O_YEAR']) \
                      .agg(SUM_PROFIT=pd.NamedAgg(column='AMOUNT', aggfunc='sum')) \
                      .reset_index()

    # Rename columns for the output
    result.rename(columns={'N_NAME': 'NATION'}, inplace=True)

    # Sort result
    result.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

    return result

def main():
    mysql_data = query_mysql()
    redis_client = DirectRedis(host='redis', port=6379, db=0)
    redis_data = query_redis(redis_client)
    result_df = calculate_profit(mysql_data, redis_data)
    result_df.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
