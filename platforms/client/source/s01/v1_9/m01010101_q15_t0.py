from pymongo import MongoClient
import mysql.connector
import pandas as pd

# MySQL connection
mydb = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)
cursor = mydb.cursor(dictionary=True)

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

pipeline = [
    {
        "$match": { 
            "L_SHIPDATE": { "$gte": '1996-01-01', "$lt": '1996-04-01' }
        }
    },
    {
        "$group": {
            "_id": "$L_SUPPKEY", 
            "TOTAL_REVENUE": { 
                "$sum": { 
                    "$multiply": ["$L_EXTENDEDPRICE", { "$subtract": [1, "$L_DISCOUNT"] }] 
                }
            }
        }
    }
]

# getting revenue from MongoDB
line_item_revenue = list(mongodb["lineitem"].aggregate(pipeline))

# getting suppliers details from MySQL
query = "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_PHONE FROM supplier"
cursor.execute(query)
suppliers = cursor.fetchall()

# perform join operation similar to SQL
df1 = pd.DataFrame(suppliers)
df2 = pd.DataFrame(line_item_revenue)
df = pd.merge(df1, df2, how='inner', left_on='S_SUPPKEY', right_on='_id')

max_revenue = df["TOTAL_REVENUE"].max()

df = df[df['TOTAL_REVENUE'] == max_revenue]

df.sort_values('S_SUPPKEY').to_csv("query_output.csv", index=False)
