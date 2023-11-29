import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]

# Connect to the Redis database
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Execute SQL query for lineitem and orders from MySQL
with mysql_conn.cursor() as cursor:
    query = """
    SELECT 
        L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT, L_SHIPDATE
    FROM 
        lineitem
    WHERE 
        L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    """
    cursor.execute(query)
    lineitem_orders_data = cursor.fetchall()

# Create a DataFrame from the fetched data
lineitem_orders_df = pd.DataFrame(lineitem_orders_data, columns=[
    'L_ORDERKEY',
    'L_EXTENDEDPRICE',
    'L_DISCOUNT',
    'L_SHIPDATE'
])

# Get suppliers and customers from MongoDB
suppliers = list(mongo_db.supplier.find({"S_NATIONKEY": {"$in": ["INDIA", "JAPAN"]}}))
customers = list(mongo_db.customer.find({"C_NATIONKEY": {"$in": ["INDIA", "JAPAN"]}}))

# Convert suppliers and customers to DataFrame
suppliers_df = pd.DataFrame(suppliers)
customers_df = pd.DataFrame(customers)

# Perform the necessary joins and calculate revenue
orders_df = pd.read_sql("SELECT O_ORDERKEY, O_CUSTKEY FROM orders", mysql_conn)

# Calculate the year from L_SHIPDATE
lineitem_orders_df['L_YEAR'] = pd.to_datetime(lineitem_orders_df['L_SHIPDATE']).dt.year

# Calculate gross discounted revenue for lineitems
lineitem_orders_df['REVENUE'] = lineitem_orders_df['L_EXTENDEDPRICE'] * (1 - lineitem_orders_df['L_DISCOUNT'])

# Merge lineitems, orders, customers, and suppliers based on the query condition
result_df = lineitem_orders_df.merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')
result_df = result_df.merge(customers_df[['C_CUSTKEY', 'C_NATIONKEY']], left_on='O_CUSTKEY', right_on='C_CUSTKEY', how='inner')
result_df = result_df.rename(columns={'C_NATIONKEY': 'CUST_NATION'})

# Filter DataFrame for the conditions specified in the query
result_df = result_df[(result_df['CUST_NATION'] == 'INDIA') | (result_df['CUST_NATION'] == 'JAPAN')]
result_df = result_df[result_df['L_YEAR'].isin([1995, 1996])]

# Use Redis to get nation information and merge with the result dataframe
nation_df = pd.read_json(redis_conn.get('nation'))
result_df = result_df.merge(nation_df, left_on='SUPP_NATION', right_on='N_NATIONKEY', how='inner')
result_df = result_df.rename(columns={'N_NAME': 'SUPP_NATION'})

# Select and order the relevant columns as requested in the query
final_df = result_df[['CUST_NATION', 'L_YEAR', 'REVENUE', 'SUPP_NATION']]
final_df = final_df.sort_values(by=['SUPP_NATION', 'CUST_NATION', 'L_YEAR'])

# Write the output to a CSV file
final_df.to_csv('query_output.csv', index=False)

# Close database connections
mysql_conn.close()
mongo_client.close()
