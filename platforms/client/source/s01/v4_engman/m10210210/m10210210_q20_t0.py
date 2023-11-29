import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)
mysql_cursor = mysql_conn.cursor()

# Connection to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']

# Connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MySQL to get partsupp and lineitem tables
mysql_cursor.execute("SELECT * FROM partsupp")
partsupp_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

mysql_cursor.execute("""
    SELECT * FROM lineitem 
    WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
""")
lineitem_df = pd.DataFrame(mysql_cursor.fetchall(), columns=['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])

# Close MySQL connection
mysql_conn.close()

# Query MongoDB to get nation and supplier tables
nation_df = pd.DataFrame(list(mongo_db.nation.find({})))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find({})))

# Query Redis to get part table as Pandas DataFrame
part_data = redis_client.get('part')
part_df = pd.read_json(part_data)

# Filtering national suppliers
canada_nationkey = nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].values[0]
canada_supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == canada_nationkey]

# Filtering parts with a forest naming convention (assuming 'forest' in the name)
part_df = part_df[part_df['P_NAME'].str.contains('forest')]

# Combine data
combined_df = pd.merge(lineitem_df, partsupp_df, how='inner', left_on=['L_SUPPKEY', 'L_PARTKEY'], right_on=['PS_SUPPKEY', 'PS_PARTKEY'])
combined_df = pd.merge(combined_df, canada_supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
combined_df = pd.merge(combined_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Group by to identify suppliers with excess parts
result_df = combined_df.groupby(['S_SUPPKEY', 'S_NAME'])['L_QUANTITY'].sum().reset_index()
result_df = result_df[result_df['L_QUANTITY'] > (result_df['L_QUANTITY'] * 0.5)]

# Writing result to CSV
result_df.to_csv('query_output.csv', index=False)
