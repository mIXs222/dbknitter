# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

def fetch_mysql_data():
    conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
    sql_query = """
    SELECT 
        DATE_FORMAT(O_ORDERDATE, '%%Y') AS order_year, 
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS revenue
    FROM 
        orders
    JOIN 
        lineitem ON O_ORDERKEY = L_ORDERKEY
    WHERE 
        DATE_FORMAT(O_ORDERDATE, '%%Y') IN ('1995', '1996')
    GROUP BY 
        order_year
    """
    df_mysql = pd.read_sql(sql_query, conn)
    conn.close()
    return df_mysql

def fetch_mongodb_data():
    client = pymongo.MongoClient('mongodb', 27017)
    db = client['tpch']
    nation_data = list(db.nation.find({"N_NAME": "INDIA"}, {"_id": 0, "N_NATIONKEY": 1}))
    nation_keys = [item['N_NATIONKEY'] for item in nation_data]
    client.close()
    return nation_keys

def fetch_redis_data(nation_keys):
    dr = DirectRedis(host='redis', port=6379, db=0)
    supplier_data = pd.read_json(dr.get('supplier').decode('utf-8'))
    indian_suppliers = supplier_data[supplier_data['S_NATIONKEY'].isin(nation_keys)]['S_SUPPKEY'].tolist()
    return indian_suppliers

def calculate_market_share(df_mysql, indian_suppliers):
    df_mysql['market_share'] = df_mysql.apply(lambda x: x['revenue'] if x['O_SUPPKEY'] in indian_suppliers else 0, axis=1)
    df_mysql['market_share'] = df_mysql['market_share'] / df_mysql['revenue'].sum()
    result = df_mysql[['order_year', 'market_share']]
    return result

# Fetch data from different databases
df_mysql = fetch_mysql_data()
nation_keys = fetch_mongodb_data()
indian_suppliers = fetch_redis_data(nation_keys)

# Calculate market share
result = calculate_market_share(df_mysql, indian_suppliers)

# Write output to CSV
result.to_csv('query_output.csv', index=False)
