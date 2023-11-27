import pymongo
import csv
from datetime import datetime, timedelta

# MongoDB connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]
customer_collection = mongo_db["customer"]
orders_collection = mongo_db["orders"]

# Helper function to execute the query
def execute_query():
    # Calculate the date 7 years ago from today
    seven_years_ago = datetime.today() - timedelta(weeks=7*52)

    # Filtered country codes
    country_codes = ['20', '40', '22', '30', '39', '42', '21']

    # Find all customers with C_PHONE starting with the country codes and their account balance greater than 0
    customers = customer_collection.find(
        {
            "C_PHONE": {"$regex": f"^(?:{'|'.join(country_codes)})"},
            "C_ACCTBAL": {"$gt": 0}
        },
        {"C_CUSTKEY": 1, "C_PHONE": 1, "C_ACCTBAL": 1}
    )
    
    # Map each customer to check if they have placed an order in the last 7 years
    # and calculate count and balance
    output_data = []
    for country_code in country_codes:
        customers_filtered = [cust for cust in customers if cust['C_PHONE'].startswith(country_code)]
        
        customer_ids = [cust["C_CUSTKEY"] for cust in customers_filtered]
        
        recent_orders = orders_collection.aggregate([
            {"$match": {
                "O_CUSTKEY": {"$in": customer_ids},
                "O_ORDERDATE": {"$gte": seven_years_ago}
            }},
            {"$group": {
                "_id": "$O_CUSTKEY"
            }}
        ])
        recent_order_ids = {order["_id"] for order in recent_orders}
        
        potential_customers = [cust for cust in customers_filtered if cust["C_CUSTKEY"] not in recent_order_ids]
            
        count = len(potential_customers)
        total_balance = sum(cust['C_ACCTBAL'] for cust in potential_customers)
        output_data.append({'Country_Code': country_code, 'Customer_Count': count, 'Total_Balance': total_balance})
    
    # Write results to a CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['Country_Code', 'Customer_Count', 'Total_Balance'])
        writer.writeheader()
        writer.writerows(output_data)

# Run the query
execute_query()
