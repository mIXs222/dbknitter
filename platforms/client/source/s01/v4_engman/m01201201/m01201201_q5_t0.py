import pymysql
import pymongo
import pandas as pd
import direct_redis

def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 database='tpch')
    with connection.cursor() as cursor:
        cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation WHERE N_REGIONKEY = (SELECT R_REGIONKEY FROM region WHERE R_NAME = 'ASIA')")
        nations = cursor.fetchall()
        cursor.execute("SELECT S_SUPPKEY, S_NATIONKEY FROM supplier")
        suppliers = cursor.fetchall()
        cursor.execute("SELECT O_ORDERKEY, O_CUSTKEY FROM orders WHERE O_ORDERDATE BETWEEN '1990-01-01' AND '1995-01-01'")
        orders = cursor.fetchall()
    connection.close()
    return nations, suppliers, orders

def get_mongodb_data():
    client = pymongo.MongoClient(host='mongodb', port=27017)
    db = client['tpch']
    region = pd.DataFrame(list(db.region.find({"R_NAME": "ASIA"}, {"_id": 0})))
    lineitem = pd.DataFrame(list(db.lineitem.find({}, {"_id": 0})))
    client.close()
    return region, lineitem

def get_redis_data():
    r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
    customer_df = r.get('customer')
    return customer_df

def main():
    nations, suppliers, orders = get_mysql_data()
    region, lineitem = get_mongodb_data()
    customer_df = get_redis_data()

    # Convert data to dataframes
    nations_df = pd.DataFrame(nations, columns=['N_NATIONKEY', 'N_NAME'])
    suppliers_df = pd.DataFrame(suppliers, columns=['S_SUPPKEY', 'S_NATIONKEY'])
    orders_df = pd.DataFrame(orders, columns=['O_ORDERKEY', 'O_CUSTKEY'])
    redis_df = pd.read_json(customer_df, orient='records')

    # Merge the dataframes
    merged_df = pd.merge(lineitem, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    merged_df = pd.merge(merged_df, redis_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')
    merged_df = pd.merge(merged_df, suppliers_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
    merged_df = pd.merge(merged_df, nations_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Filter by ASIA region and between the dates
    filtered_df = merged_df[merged_df['S_NATIONKEY'].isin(region['R_REGIONKEY'])]

    # Calculate the revenue volume
    filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])
    result = filtered_df.groupby('N_NAME')['REVENUE'].sum().reset_index()

    # Sort and select columns
    result = result.sort_values(by='REVENUE', ascending=False)

    # Output to CSV
    result.to_csv('query_output.csv', index=False, columns=['N_NAME', 'REVENUE'])

if __name__== "__main__":
    main()
