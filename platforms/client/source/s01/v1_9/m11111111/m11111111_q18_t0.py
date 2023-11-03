from pymongo import MongoClient
import pandas as pd

# Create a connection to MongoDB
client = MongoClient("mongodb://mongodb:27017/")

# Select the database
db = client["tpch"]

def get_data():
    # Aggregate query
    pipeline = [
        {"$match": {
            "$expr": {
                "$gt": ["$L_QUANTITY", 300]
            }
        }},
        {"$group": {
            "_id": "$L_ORDERKEY",
            "total_quantity": {"$sum": "$L_QUANTITY"}
        }}
    ]

    # Run aggregation pipeline and fetch result.
    result = list(db.lineitem.aggregate(pipeline))

    # Fetch data from orders and customer table.
    orders_data = list(db.orders.find({"O_ORDERKEY": {"$in": [r["_id"] for r in result]}}))
    customer_data = list(db.customer.find())
    
    orders_df = pd.DataFrame(orders_data)
    customer_df = pd.DataFrame(customer_data)

    # Join dataframes on common column.
    combined_df = pd.merge(orders_df, customer_df, left_on="O_CUSTKEY", right_on="C_CUSTKEY")

    # Filter columns and Write data to csv file.
    combined_df[["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"]].to_csv('query_output.csv', index=False)

get_data()
