import pymysql
import pymongo
import pandas as pd
import direct_redis

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
mysql_cursor = mysql_conn.cursor()

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# MySQL query
mysql_query = """
SELECT P_PARTKEY, P_TYPE,
       S_SUPPKEY,
       L_EXTENDEDPRICE, L_DISCOUNT, L_ORDERKEY, L_PARTKEY,
       O_ORDERDATE, O_ORDERKEY, O_CUSTKEY, 
       C_CUSTKEY, C_NATIONKEY
FROM part, supplier, lineitem, orders, customer
WHERE P_PARTKEY = L_PARTKEY
  AND S_SUPPKEY = L_SUPPKEY
  AND L_ORDERKEY = O_ORDERKEY
  AND O_CUSTKEY = C_CUSTKEY
  AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
  AND P_TYPE = 'SMALL PLATED COPPER';
"""
mysql_cursor.execute(mysql_query)
mysql_results = mysql_cursor.fetchall()
mysql_df = pd.DataFrame(mysql_results, columns=['P_PARTKEY', 'P_TYPE', 'S_SUPPKEY', 'L_EXTENDEDPRICE',
                                                'L_DISCOUNT', 'L_ORDERKEY', 'L_PARTKEY', 'O_ORDERDATE',
                                                'O_ORDERKEY', 'O_CUSTKEY', 'C_CUSTKEY', 'C_NATIONKEY'])

# MongoDB query for regions and nation
mongo_region = list(mongo_db.region.find({'R_NAME': 'ASIA'}))
mongo_region_df = pd.DataFrame(mongo_region)
if not mongo_region_df.empty:
    asian_region_keys = mongo_region_df['R_REGIONKEY'].tolist()
    mongo_nations = list(mongo_db.nation.find({'N_REGIONKEY': {'$in': asian_region_keys}}))
    mongo_nations_df = pd.DataFrame(mongo_nations)

    # Merge with MySQL data
    merged_df = pd.merge(mysql_df, mongo_nations_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

    # Redis query for nations
    nations_data = pd.read_msgpack(redis_conn.get('nation'))
    nations_df = pd.DataFrame(nations_data)

    # Final merge with nation data from Redis
    final_df = pd.merge(merged_df, nations_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

    # Continue to process the dataset and calculate the market share
    final_df['O_YEAR'] = pd.to_datetime(final_df['O_ORDERDATE']).dt.year
    final_df['VOLUME'] = final_df['L_EXTENDEDPRICE'] * (1 - final_df['L_DISCOUNT'])
    final_df['NATION'] = final_df['N_NAME_y']

    result_df = (final_df.groupby('O_YEAR')
                 .apply(lambda x: pd.Series({
                     'MKT_SHARE': x[x['NATION'] == 'INDIA']['VOLUME'].sum() / x['VOLUME'].sum()}))
                 .reset_index())

    # Write the final result to a CSV file
    result_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()
