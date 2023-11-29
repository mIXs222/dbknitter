# multi_db_query.py

import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MySQL
with mysql_conn.cursor() as cursor:
    # Get data related to lineitems for the product type 'SMALL PLATED COPPER'
    lineitem_query = '''
    SELECT
        YEAR(L_SHIPDATE) AS order_year,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_SUPPKEY
    FROM
        lineitem
    WHERE
        L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
        AND L_PARTKEY IN (SELECT P_PARTKEY FROM part WHERE P_TYPE = 'SMALL PLATED COPPER')
    '''
    cursor.execute(lineitem_query)
    lineitem_data = cursor.fetchall()
    lineitem_columns = [column[0] for column in cursor.description]

lineitem_df = pd.DataFrame(lineitem_data, columns=lineitem_columns)
mysql_conn.close()

# Retrieve data from MongoDB
supplier_data = list(mongo_db.supplier.find({'S_NATIONKEY': mongo_db.nation.find_one({'N_NAME': 'INDIA'})['N_NATIONKEY']}))
supplier_df = pd.DataFrame(supplier_data).rename(columns={'_id': 'S_SUPPKEY'})

# Retrieve data from Redis
part_df = pd.read_csv(redis_conn.get('part'))
part_df = part_df[part_df.P_TYPE == 'SMALL PLATED COPPER']

# Combine the data
combined_df = pd.merge(lineitem_df, supplier_df, how='inner', on='L_SUPPKEY')
combined_df['revenue'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

# Calculate market share for India within Asia for the years 1995 and 1996
market_share_df = combined_df.groupby('order_year')['revenue'].sum().reset_index(name='market_share')
market_share_df['market_share'] /= market_share_df['market_share'].sum()

# Save result to CSV
market_share_df.to_csv('query_output.csv', index=False)
