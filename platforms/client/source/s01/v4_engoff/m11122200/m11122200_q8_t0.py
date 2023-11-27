# python_code.py

import pymysql
import pandas as pd
import pymongo
import direct_redis

def get_mysql_data(connection_info):
    connection = pymysql.connect(
        host=connection_info['hostname'],
        user=connection_info['username'],
        password=connection_info['password'],
        db=connection_info['database'])
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT o.O_ORDERDATE,
                    l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) as revenue
                FROM orders o
                JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
                WHERE YEAR(o.O_ORDERDATE) IN (1995, 1996);
            """
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()
    return pd.DataFrame(result, columns=['O_ORDERDATE', 'revenue'])

def get_mongodb_data(connection_info):
    client = pymongo.MongoClient(connection_info['hostname'], connection_info['port'])
    db = client[connection_info['database']]
    part = pd.DataFrame(list(db.part.find({"P_TYPE": "SMALL PLATED COPPER"})))
    region = pd.DataFrame(list(db.region.find({"R_NAME": "ASIA"})))
    nation = pd.DataFrame(list(db.nation.find({"N_NAME": "INDIA"})))
    client.close()
    return part, region, nation

def get_redis_data(connection_info):
    direct_redis_conn = direct_redis.DirectRedis(host=connection_info['hostname'], port=connection_info['port'], db=connection_info['database'])
    supplier = pd.read_json(direct_redis_conn.get('supplier'))
    return supplier

def main():
    mysql_conn_info = {'database': 'tpch', 'username': 'root', 'password': 'my-secret-pw', 'hostname': 'mysql'}
    mongodb_conn_info = {'database': 'tpch', 'port': 27017, 'hostname': 'mongodb'}
    redis_conn_info = {'database': 0, 'port': 6379, 'hostname': 'redis'}
    
    orders_lineitem_data = get_mysql_data(mysql_conn_info)
    part_data, region_data, nation_data = get_mongodb_data(mongodb_conn_info)
    supplier_data = get_redis_data(redis_conn_info)

    # Merge and calculate market share
    supplier_data = supplier_data.merge(nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY', how='inner')
    orders_lineitem_data['year'] = pd.to_datetime(orders_lineitem_data['O_ORDERDATE']).dt.year
    market_share = {}
    for year in [1995, 1996]:
        yearly_data = orders_lineitem_data[orders_lineitem_data['year'] == year]
        total_revenue = yearly_data['revenue'].sum()
        supplier_revenue = yearly_data.join(supplier_data.set_index('S_NATIONKEY'), on='S_NATIONKEY', how='inner')['revenue'].sum()
        market_share[year] = supplier_revenue / total_revenue if total_revenue else 0

    # Write the results to a CSV file
    pd.DataFrame([market_share]).to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
