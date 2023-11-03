import pymongo
import pandas as pd

def mongodb_to_dataframe(db_name, collection_name, query, host="localhost", port=27017, username=None, password=None):
    """ Create a dataframe from mongodb collection """
    client = pymongo.MongoClient(host, username=username, password=password)
    db = client[db_name]
    cursor = db[collection_name].find(query)
    df = pd.DataFrame(list(cursor))
    return df

# Create dataframes for required tables
db_name = "tpch"

customer_df = mongodb_to_dataframe(db_name, "customer", {})
orders_df = mongodb_to_dataframe(db_name, "orders", {})

# Select customers with required phone codes and account balance > average
customer_df["CNTRYCODE"] = customer_df["C_PHONE"].str[:2]
avg_acctbal = customer_df[(customer_df["C_ACCTBAL"] > 0) & customer_df["CNTRYCODE"].isin(['20', '40', '22', '30', '39', '42', '21'])]["C_ACCTBAL"].mean()
customer_df = customer_df[(customer_df["CNTRYCODE"].isin(['20', '40', '22', '30', '39', '42', '21'])) & (customer_df["C_ACCTBAL"] > avg_acctbal)]

# Remove customers who exists in orders table
customer_df = customer_df[~customer_df["C_CUSTKEY"].isin(orders_df["O_CUSTKEY"])]

# Group by country code and compute sum of account balance and count
grouped_df = customer_df.groupby("CNTRYCODE").agg({"C_CUSTKEY": "count", "C_ACCTBAL": "sum"}).reset_index()
grouped_df.rename(columns={"C_CUSTKEY": "NUMCUST", "C_ACCTBAL": "TOTACCTBAL"}, inplace=True)

# Write results to CSV
grouped_df.to_csv("query_output.csv", index=False)
