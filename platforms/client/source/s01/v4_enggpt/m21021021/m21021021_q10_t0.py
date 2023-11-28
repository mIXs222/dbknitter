import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis
    
def get_mysql_data(connection):
    query = """
    SELECT customer.C_CUSTKEY, customer.C_NAME, customer.C_ACCTBAL, 
           customer.C_ADDRESS, customer.C_PHONE, customer.C_COMMENT
    FROM customer
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
    return pd.DataFrame(data, columns=['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'])

def get_mongodb_data(client):
    lineitem_collection = client.tpch.lineitem
    lineitems = list(lineitem_collection.find({
        'L_RETURNFLAG': 'R',
        'L_SHIPDATE': {'$gte': '1993-10-01', '$lte': '1993-12-31'}
    }, {'_id': 0}))
    df_lineitem = pd.DataFrame(lineitems)
    df_lineitem['REVENUE'] = df_lineitem['L_EXTENDEDPRICE'] * (1 - df_lineitem['L_DISCOUNT'])
    return df_lineitem

def get_redis_data(redis_client):
    nations = pd.read_json(redis_client.get('nation').decode('utf-8'))
    orders = pd.read_json(redis_client.get('orders').decode('utf-8'))
    orders = orders[(orders.O_ORDERDATE >= '1993-10-01') & (orders.O_ORDERDATE <= '1993-12-31')]
    return nations, orders

def main():
    # Connect to MySQL
    mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

    # Connect to MongoDB
    mongodb_client = pymongo.MongoClient('mongodb', 27017)
    
    # Connect to Redis
    redis_client = DirectRedis(port=6379, host='redis')

    # Fetch data from different databases
    df_customer = get_mysql_data(mysql_conn)
    df_lineitem = get_mongodb_data(mongodb_client)
    nations, df_orders = get_redis_data(redis_client)

    # Close connections
    mysql_conn.close()
    mongodb_client.close()

    # Merge the dataframes
    df_merged = pd.merge(df_customer, df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    df_merged = pd.merge(df_merged, df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
    df_merged = pd.merge(df_merged, nations, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

    # Calculate total revenue per customer and keep necessary columns
    df_result = df_merged.groupby(['C_CUSTKEY', 'C_NAME', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT']).agg({'REVENUE': 'sum'}).reset_index()
    df_result.sort_values(by=['REVENUE', 'C_CUSTKEY', 'C_NAME'], ascending=[True, True, True], inplace=True)
    df_result.sort_values(by='C_ACCTBAL', ascending=False, inplace=True)

    # Write results to csv
    df_result.to_csv('query_output.csv', index=False)

if __name__ == '__main__':
    main()
