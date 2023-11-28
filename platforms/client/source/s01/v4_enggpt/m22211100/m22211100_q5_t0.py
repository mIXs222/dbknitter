import pymysql
import pymongo
import pandas as pd

# Connect to MySQL Server
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Read order and lineitem tables from MySQL
mysql_query = """
    SELECT o.O_ORDERKEY, o.O_CUSTKEY, l.L_EXTENDEDPRICE, l.L_DISCOUNT, l.L_ORDERKEY
    FROM orders o
    JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
    WHERE o.O_ORDERDATE BETWEEN '1990-01-01' AND '1994-12-31'
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    order_lineitem_data = cursor.fetchall()

# Convert to pandas DataFrame
order_lineitem_df = pd.DataFrame(order_lineitem_data, columns=['O_ORDERKEY', 'O_CUSTKEY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_ORDERKEY'])

# Close MySQL connection
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://root:my-secret-pw@mongodb:27017/tpch")
mongodb = mongo_client['tpch']
customer_col = mongodb['customer']

# Convert customers from MongoDB to pandas DataFrame
customer_df = pd.DataFrame(list(customer_col.find()))

# Establish connection with Redis
import direct_redis

redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read nation and region tables from Redis
nation_df = pd.read_json(redis_conn.get('nation').decode('utf-8'))
region_df = pd.read_json(redis_conn.get('region').decode('utf-8'))

# Filtering customers by 'ASIA' region
asia_region_keys = region_df[region_df['R_NAME'] == 'ASIA']['R_REGIONKEY'].tolist()
asia_nation_keys = nation_df[nation_df['N_REGIONKEY'].isin(asia_region_keys)]['N_NATIONKEY'].tolist()
asia_customers = customer_df[customer_df['C_NATIONKEY'].isin(asia_nation_keys)]

# Combining the data
combined_data = order_lineitem_df.merge(asia_customers, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Compute total revenue and group by nation
combined_data['REVENUE'] = combined_data['L_EXTENDEDPRICE'] * (1 - combined_data['L_DISCOUNT'])
revenue_by_nation = combined_data.groupby('C_NATIONKEY')['REVENUE'].sum().reset_index()

# Merge with nation names
revenue_nation_names = revenue_by_nation.merge(nation_df, left_on='C_NATIONKEY', right_on='N_NATIONKEY')

# Ordering by revenue in descending order
final_result = revenue_nation_names[['N_NAME', 'REVENUE']].sort_values(by='REVENUE', ascending=False)

# Write results to CSV
final_result.to_csv('query_output.csv', index=False)
