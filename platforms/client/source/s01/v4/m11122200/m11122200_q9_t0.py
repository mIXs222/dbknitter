import pandas as pd
import pymysql
import pymongo
from direct_redis import DirectRedis

# Function to execute SQL query in MySQL
def execute_sql_query(sql, connection):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# MySQL Query
mysql_query = """
SELECT
    orders.O_ORDERKEY, orders.O_ORDERDATE,
    lineitem.L_ORDERKEY, lineitem.L_PARTKEY,
    lineitem.L_SUPPKEY, lineitem.L_EXTENDEDPRICE, 
    lineitem.L_DISCOUNT, lineitem.L_QUANTITY
FROM 
    orders, lineitem
WHERE
    orders.O_ORDERKEY = lineitem.L_ORDERKEY;
"""
mysql_data = execute_sql_query(mysql_query, mysql_conn)
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
# Query MongoDB for nation collection
nation_data = pd.DataFrame(list(mongo_db.nation.find({}, {'_id': False})))
# Query MongoDB for part collection
part_data = pd.DataFrame(list(mongo_db.part.find({'P_NAME': {'$regex': '.*dim.*'}}, {'_id': False})))
mongo_client.close()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_data = pd.read_json(redis_client.get('supplier'))
partsupp_data = pd.read_json(redis_client.get('partsupp'))
redis_client.close()

# Merge dataframes to prepare for accommodating query
merged_data = mysql_data.merge(part_data, left_on='L_PARTKEY', right_on='P_PARTKEY').merge(
    supplier_data, left_on='L_SUPPKEY', right_on='S_SUPPKEY').merge(
    partsupp_data, on=['PS_PARTKEY', 'PS_SUPPKEY']).merge(
    nation_data, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate derived columns and group by logic
merged_data['O_YEAR'] = pd.to_datetime(merged_data['O_ORDERDATE']).dt.year
merged_data['AMOUNT'] = (merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])) - (
    merged_data['PS_SUPPLYCOST'] * merged_data['L_QUANTITY'])

grouped_data = merged_data.groupby(['N_NAME', 'O_YEAR']).agg(SUM_PROFIT=('AMOUNT', 'sum')).reset_index()

# Order the results as requested
grouped_data.sort_values(by=['N_NAME', 'O_YEAR'], ascending=[True, False], inplace=True)

# Write the result to a csv file
grouped_data.to_csv("query_output.csv", index=False)
