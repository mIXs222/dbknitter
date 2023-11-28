# Python code to execute the complex cross-database query
import pymysql
import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Function to get MySQL data
def get_mysql_data(connection_info):
    connection = pymysql.connect(host=connection_info['hostname'],
                                 user=connection_info['username'],
                                 password=connection_info['password'],
                                 db=connection_info['database_name'])
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT o.O_ORDERDATE, l.L_EXTENDEDPRICE, l.L_DISCOUNT, l.L_QUANTITY, l.L_ORDERKEY, p.PS_SUPPLYCOST, p.PS_PARTKEY
            FROM lineitem l
            JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
            JOIN partsupp p ON l.L_PARTKEY = p.PS_PARTKEY AND l.L_SUPPKEY = p.PS_SUPPKEY
            """
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()
    
    # Convert to Pandas DataFrame
    df = pd.DataFrame(list(result), columns=['O_ORDERDATE', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_QUANTITY', 'L_ORDERKEY', 'PS_SUPPLYCOST', 'PS_PARTKEY'])
    return df

# Function to get MongoDB data
def get_mongodb_data(connection_info):
    client = pymongo.MongoClient(host=connection_info['hostname'], port=connection_info['port'])
    db = client[connection_info['database_name']]
    
    supplier_data = pd.DataFrame(list(db.supplier.find()))
    supplier_data = supplier_data.rename(columns={'S_SUPPKEY': 'L_SUPPKEY', 'S_NATIONKEY': 'N_NATIONKEY'})
    
    return supplier_data

# Function to get Redis data
def get_redis_data(connection_info):
    r = DirectRedis(host=connection_info['hostname'], port=connection_info['port'], db=connection_info['database_name'])
    
    nation_data = pd.read_msgpack(r.get('nation'))
    part_data = pd.read_msgpack(r.get('part'))
    part_data = part_data[part_data['P_NAME'].str.contains('dim')]
    
    return nation_data.set_index('N_NATIONKEY'), part_data.set_index('P_PARTKEY')

# Connection information
mysql_info = {'hostname': 'mysql', 'username': 'root', 'password': 'my-secret-pw', 'database_name': 'tpch'}
mongodb_info = {'hostname': 'mongodb', 'port': 27017, 'database_name': 'tpch'}
redis_info = {'hostname': 'redis', 'port': 6379, 'database_name': 0}

# Get data from different sources
mysql_df = get_mysql_data(mysql_info)
mongodb_df = get_mongodb_data(mongodb_info)
nation_df, part_df = get_redis_data(redis_info)

# Join data from different sources
result = mysql_df.join(part_df, on='PS_PARTKEY')
result = result.join(mongodb_df, on='L_SUPPKEY')
result = result.join(nation_df, on='N_NATIONKEY')

# Perform the profit calculation
result['profit'] = (result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])) - (result['L_QUANTITY'] * result['PS_SUPPLYCOST'])

# Group by nation and year
result['year'] = pd.to_datetime(result['O_ORDERDATE']).dt.year
final_result = result.groupby(['N_NAME', 'year'])['profit'].sum().reset_index()

# Sort the results
final_result = final_result.sort_values(by=['N_NAME', 'year'], ascending=[True, False])

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
