# Import necessary libraries
import pymongo
import pymysql
import pandas as pd
from decimal import Decimal

# Function to fetch avg account balance from MongoDB
def get_average_account_balance(mongo_db):
    pipeline = [
        {"$match": {"C_ACCTBAL": {"$gt": 0.00}, "C_PHONE": {"$regex": "^(20|40|22|30|39|42|21)"}}},
        {"$group": {"_id": None, "avg_acct_bal": {"$avg": "$C_ACCTBAL"}}}
    ]
    avg_result = list(mongo_db.customer.aggregate(pipeline))
    return avg_result[0]['avg_acct_bal'] if avg_result else 0

# Function to check if a customer has orders in the MySQL database
def customer_has_no_orders(mysql_db, custkey):
    with mysql_db.cursor() as cursor:
        cursor.execute("SELECT O_CUSTKEY FROM orders WHERE O_CUSTKEY = %s", (custkey,))
        return cursor.rowcount == 0

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
avg_acct_bal = get_average_account_balance(mongo_db)

# Connect to MySQL
mysql_conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Querying customers from MongoDB
cust_query = {
    "C_ACCTBAL": {"$gt": Decimal(str(avg_acct_bal))},
    "C_PHONE": {"$regex": "^(20|40|22|30|39|42|21)"}
}
customers = pd.DataFrame(list(mongo_db.customer.find(cust_query)))

# Filtering customers who do not have orders
customers['NO_ORDERS'] = customers.apply(lambda row: customer_has_no_orders(mysql_conn, row['C_CUSTKEY']), axis=1)
customers = customers[customers['NO_ORDERS']]

# Aggregation to match the SQL query
result = customers.groupby(customers['C_PHONE'].str[:2]).agg(
    NUMCUST=('C_CUSTKEY', 'count'),
    TOTACCTBAL=('C_ACCTBAL', 'sum')
).reset_index().rename(columns={'C_PHONE': 'CNTRYCODE'})

# Save to CSV
result.to_csv('query_output.csv', index=False)

# Closing connections
mongo_client.close()
mysql_conn.close()
