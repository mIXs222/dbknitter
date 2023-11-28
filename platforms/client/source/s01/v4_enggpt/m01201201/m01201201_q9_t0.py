import pymysql
import pymongo
import pandas as pd
import direct_redis

# Establish a connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Establish a connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Establish a connection to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch from MySQL
with mysql_conn.cursor() as cursor:
    # Fetch suppliers
    cursor.execute("SELECT S_SUPPKEY, S_NATIONKEY FROM supplier")
    suppliers = pd.DataFrame(cursor.fetchall(), columns=['S_SUPPKEY', 'S_NATIONKEY'])
    
    # Fetch nations
    cursor.execute("SELECT N_NATIONKEY, N_NAME FROM nation")
    nations = pd.DataFrame(cursor.fetchall(), columns=['N_NATIONKEY', 'N_NAME'])
    
    # Fetch orders
    cursor.execute("SELECT O_ORDERKEY, O_ORDERDATE FROM orders")
    orders = pd.DataFrame(cursor.fetchall(), columns=['O_ORDERKEY', 'O_ORDERDATE'])
    
mysql_conn.close()

# Fetch Partsupp and Lineitem from MongoDB
partsupp_df = pd.DataFrame(list(mongodb.partsupp.find()))
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find()))

# Fetch Part from Redis
part_data = redis_conn.get('part')
part_df = pd.read_csv(pd.compat.StringIO(part_data.decode()), sep=',')

# Filter parts for 'dim' in the name
dim_part_df = part_df[part_df['P_NAME'].str.contains('dim', case=False, na=False)]

# Join the DataFrames to find relevant line items
result_df = (lineitem_df.merge(dim_part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
                        .merge(partsupp_df, on=['L_PARTKEY', 'L_SUPPKEY'])
                        .merge(suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
                        .merge(nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
                        .merge(orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY'))

# Calculate profit for each line item
result_df['profit'] = (result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])
                        - result_df['PS_SUPPLYCOST'] * result_df['L_QUANTITY'])

# Parse order date to extract year
result_df['year'] = pd.to_datetime(result_df['O_ORDERDATE']).dt.year

# Group by nation and year
grouped_result = result_df.groupby(['N_NAME', 'year'])['profit'].sum().reset_index()

# Sorting the results as required
sorted_result = grouped_result.sort_values(by=['N_NAME', 'year'], ascending=[True, False])

# Output the results to a CSV file
sorted_result.to_csv('query_output.csv', index=False)
