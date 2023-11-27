# query_execution.py
import pymysql
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    passwd='my-secret-pw',
    db='tpch',
)
mysql_cursor = mysql_conn.cursor()

# Execute MySQL query
mysql_query = """
SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_COMMENT
FROM customer
"""
mysql_cursor.execute(mysql_query)
customers = pd.DataFrame(mysql_cursor.fetchall(), columns=["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_COMMENT"])

mysql_cursor.close()
mysql_conn.close()

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_orders = mongo_db["orders"]
mongo_lineitem = mongo_db["lineitem"]

# Execute MongoDB queries
start_date = datetime(1993, 10, 1)
end_date = datetime(1994, 1, 1)

# Find orders within the specified quarter
orders_in_range = mongo_orders.find(
    {
        "O_ORDERDATE": {"$gte": start_date, "$lt": end_date}
    },
    {"O_ORDERKEY": 1, "O_CUSTKEY": 1}
)
orders_in_range_df = pd.DataFrame(list(orders_in_range))

# Find lineitems corresponding to those orders
lineitems = mongo_lineitem.aggregate([
    {
        "$match": {
            "L_ORDERKEY": {"$in": orders_in_range_df["O_ORDERKEY"].tolist()},
            "L_RETURNFLAG": {"$eq": "R"}
        }
    },
    {
        "$group": {
            "_id": "$L_ORDERKEY",
            "lost_revenue": {
                "$sum": {
                    "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
                }
            }
        }
    }
])

lineitems_df = pd.DataFrame(list(lineitems)).rename(columns={"_id": "L_ORDERKEY"})

# Mapping customer keys to order keys for lost revenue calculation
order_cust_mapping = orders_in_range_df.set_index("O_ORDERKEY").to_dict()["O_CUSTKEY"]
lineitems_df["C_CUSTKEY"] = lineitems_df["L_ORDERKEY"].map(order_cust_mapping)

# Connect to Redis and retrieve nation data
redis_client = DirectRedis(host='redis', port=6379, db=0)
nation_data = redis_client.get('nation')
nation_df = pd.DataFrame(eval(nation_data))

# Merge DataFrames
merged_df = customers.merge(lineitems_df, on="C_CUSTKEY", how="inner")
merged_df = merged_df.merge(nation_df, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="left")

# Select and sort data
output_df = merged_df[["C_NAME", "C_ADDRESS", "N_NAME", "C_PHONE", "C_ACCTBAL", "C_COMMENT", "lost_revenue"]]
output_df = output_df.sort_values(by=["lost_revenue", "C_CUSTKEY", "C_NAME", "C_ACCTBAL"], ascending=[False, True, True, True])

# Write to CSV
output_df.to_csv("query_output.csv", index=False)
