import pandas as pd
import pymysql
import pymongo

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# Execute MySQL query
mysql_query = """
SELECT C_CUSTKEY, C_NAME, SUBSTR(C_PHONE, 1, 2) as CNTRYCODE, C_ACCTBAL
FROM customer
WHERE C_ACCTBAL > (
        SELECT AVG(C_ACCTBAL)
        FROM customer
        WHERE C_ACCTBAL > 0
        AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
    )
AND NOT EXISTS (
        SELECT O_CUSTKEY
        FROM orders
        WHERE customer.C_CUSTKEY = O_CUSTKEY
    )
AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(mysql_query)
    customer_results = cursor.fetchall()

# Transform MySQL records into DataFrame
customers_df = pd.DataFrame(customer_results, columns=["C_CUSTKEY", "C_NAME", "CNTRYCODE", "C_ACCTBAL"])

# Exclude customers who have placed orders
order_cust_keys = list(orders_collection.find({}, {"O_CUSTKEY": 1}))
order_cust_keys = [doc["O_CUSTKEY"] for doc in order_cust_keys]
customers_df = customers_df[~customers_df["C_CUSTKEY"].isin(order_cust_keys)]

# Aggregate results by country codes
result_df = customers_df.groupby("CNTRYCODE").agg(
    NUMCUST=pd.NamedAgg(column="C_CUSTKEY", aggfunc="count"),
    TOTACCTBAL=pd.NamedAgg(column="C_ACCTBAL", aggfunc="sum")
).reset_index()

# Save the results to CSV
result_df.to_csv('query_output.csv', index=False)
