import pandas as pd
import mysql.connector
import pymongo

# Connect to MySQL Server
mysql = mysql.connector.connect(user='root',
                                password='my-secret-pw',
                                host='mysql',
                                database='tpch')
cursor = mysql.cursor(buffered=True)

# Fetch part data from MySQL
cursor.execute("""SELECT P_PARTKEY, P_TYPE FROM part""")
part_data = cursor.fetchall()

# Convert data to pandas DataFrame
part_df = pd.DataFrame(part_data, columns=['P_PARTKEY', 'P_TYPE'])

# Connect to MongoDB Server
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client['tpch']

# Fetch lineitem data from MongoDB
lineitem_data = list(mongodb['lineitem'].find({},{
    "L_PARTKEY": 1,
    "L_EXTENDEDPRICE": 1,
    "L_DISCOUNT": 1,
    "L_SHIPDATE": 1
}))

# Convert data to pandas DataFrame
lineitem_df = pd.DataFrame(lineitem_data)

# Merge the two dataframes
merged_df = pd.merge(left=lineitem_df, right=part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter data by the shipdate
filtered_df = merged_df.loc[(merged_df['L_SHIPDATE'] >= '1995-09-01') & (merged_df['L_SHIPDATE'] < '1995-10-01')]

# Calculate PROMO_REVENUE
filtered_df['PROMO_REVENUE'] = filtered_df.apply(lambda row: row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT']) if row['P_TYPE'].startswith('PROMO') else 0, axis=1)
promo_revenue = 100.00 * filtered_df['PROMO_REVENUE'].sum() / (filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])).sum()

# Create a DataFrame for PROMO_REVENUE and output to csv file
promo_revenue_df = pd.DataFrame([promo_revenue], columns=['PROMO_REVENUE'])
promo_revenue_df.to_csv('query_output.csv', index=False)
