import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL (for Nation information)
mysql_connection = pymysql.connect(host='mysql', 
                                   user='root', 
                                   password='my-secret-pw', 
                                   database='tpch')
mysql_cursor = mysql_connection.cursor()
mysql_cursor.execute("SELECT N_NATIONKEY FROM nation WHERE N_NAME='SAUDI ARABIA'")
nation_result = mysql_cursor.fetchone()
saudi_arabia_nationkey = nation_result[0]
mysql_cursor.close()
mysql_connection.close()

# Connect to MongoDB (for Orders and Lineitem information)
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]
orders = list(mongodb_db.orders.find({"O_ORDERSTATUS": "F"}))
orders_dict = {order["O_ORDERKEY"]: order for order in orders}

# Fetch Lineitem data
lineitem_data = mongodb_db.lineitem.find({"L_RETURNFLAG": "R",
                                          "L_ORDERKEY": {"$in": list(orders_dict.keys())}})
lineitem_df = pd.DataFrame(lineitem_data)

# Get multi-supplier orders
multi_supplier_orders = lineitem_df[
    lineitem_df.duplicated(subset=['L_ORDERKEY'], keep=False)
]['L_ORDERKEY'].unique()

# Filter multi-supplier lineitems
lineitem_df = lineitem_df[lineitem_df['L_ORDERKEY'].isin(multi_supplier_orders)]

# Connect to Redis (for Supplier information)
redis_db = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.DataFrame(eval(redis_db.get("supplier")))

# Filter suppliers from Saudi Arabia
saudi_suppliers = supplier_df[supplier_df['S_NATIONKEY'] == saudi_arabia_nationkey]

# Combine the data and calculate the NUMWAIT
combined_df = lineitem_df.merge(saudi_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
combined_df = combined_df.groupby('S_NAME').agg(NUMWAIT=pd.NamedAgg(column="L_ORDERKEY", aggfunc="count"))
combined_df = combined_df.reset_index()

# Sort the results
combined_df = combined_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to CSV
combined_df.to_csv("query_output.csv", index=False)
