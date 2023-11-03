import mysql.connector
import pymongo
import pandas as pd

# MySQL connection
mydb = mysql.connector.connect(
    host="mysql",
    user="root",
    password='my-secret-pw',
    database='tpch'
)

# MongoDB connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]

# get the lineitem MongoDB collection
lineitem_collection = mongo_db["lineitem"]

# get the orders MongoDB collection
orders_collection = mongo_db["orders"]

# Empty dataframe creation for storing joined dataframe
df = pd.DataFrame()

# Iterating through each document in lineitem collection
for lineitem in lineitem_collection.find({}):
    # Filter related orders data from MySQL
    mycursor = mydb.cursor()
    query = "SELECT * FROM orders WHERE O_ORDERKEY = {}".format(lineitem['L_ORDERKEY'])
    mycursor.execute(query)
    orders_data = mycursor.fetchall()

    # Creating dataframe for each lineitem and related orders data
    temp_df = pd.DataFrame(orders_data, columns=[desc[0] for desc in mycursor.description])
    temp_df = temp_df.assign(**lineitem)

    # Appending above dataframe to final dataframe
    df = df.append(temp_df, ignore_index=True)

# Perform operations as per the query description
result = df[(df.L_SHIPMODE.isin(['MAIL', 'SHIP'])) &
            (df.L_COMMITDATE < df.L_RECEIPTDATE) &
            (df.L_SHIPDATE < df.L_COMMITDATE) &
            (df.L_RECEIPTDATE >= '1994-01-01') &
            (df.L_RECEIPTDATE < '1995-01-01')]

result['HIGH_LINE_COUNT'] = result.apply(
    lambda x: 1 if x['O_ORDERPRIORITY'] == '1-URGENT' or x['O_ORDERPRIORITY'] == '2-HIGH' else 0,
    axis=1
)

result['LOW_LINE_COUNT'] = result.apply(
    lambda x: 1 if x['O_ORDERPRIORITY'] != '1-URGENT' and x['O_ORDERPRIORITY'] != '2-HIGH' else 0,
    axis=1
)

output = result.groupby('L_SHIPMODE').agg({'HIGH_LINE_COUNT': 'sum', 'LOW_LINE_COUNT': 'sum'})

# Write result to csv
output.to_csv('query_output.csv')
