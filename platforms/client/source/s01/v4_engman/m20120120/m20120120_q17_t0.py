import pymysql
import pymongo
import pandas as pd

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# MongoDB connection parameters
mongodb_params = {
    'host': 'mongodb',
    'port': 27017,
    'db': 'tpch'
}

# Establish connection to MySQL
mysql_conn = pymysql.connect(**mysql_params)
cursor = mysql_conn.cursor()

# Execute MySQL query to get lineitem data
mysql_query = """
SELECT L_PARTKEY, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT
FROM lineitem
"""
cursor.execute(mysql_query)
lineitem_data = cursor.fetchall()

# Process the data to a DataFrame (without DictCursor)
df_lineitem = pd.DataFrame(lineitem_data, columns=['L_PARTKEY', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT'])

# Close MySQL cursor and connection
cursor.close()
mysql_conn.close()

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient(**{k: mongodb_params[k] for k in ['host', 'port']})
mongo_db = mongo_client[mongodb_params['db']]

# Execute MongoDB query to get part data
mongo_query = {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'}
part_data = mongo_db.part.find(mongo_query, {'P_PARTKEY': 1, '_id': 0})

# Process the data to a DataFrame
df_part = pd.DataFrame(list(part_data))

# Merge the MySQL and MongoDB data on part key
merged_data = pd.merge(df_lineitem, df_part, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the average quantity
average_quantity = merged_data['L_QUANTITY'].mean()

# Filter out the parts which quantity is less than 20% of average
filtered_data = merged_data[merged_data['L_QUANTITY'] < 0.2 * average_quantity]

# Calculate the average yearly loss
filtered_data['YEARLY_LOSS'] = filtered_data['L_EXTENDEDPRICE'] * (1 - filtered_data['L_DISCOUNT'])
average_yearly_loss = filtered_data['YEARLY_LOSS'].sum() / 7  # Considering data for 7 years

# Write result to CSV
output_df = pd.DataFrame({'Average_Yearly_Loss': [average_yearly_loss]})
output_df.to_csv('query_output.csv', index=False)
