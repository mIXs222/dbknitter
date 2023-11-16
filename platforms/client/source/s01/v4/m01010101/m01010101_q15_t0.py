import pymongo
import csv
from datetime import datetime

# Function to calculate revenue
def calculate_revenue(line_items):
    return sum([item['L_EXTENDEDPRICE'] * (1 - item['L_DISCOUNT']) for item in line_items])

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Fetch lineitem and supplier data
lineitem = db["lineitem"]
supplier = db["supplier"]

# Date range
start_date = datetime.strptime('1996-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1996-04-01', '%Y-%m-%d')

# Process lineitem data for the given date range
lineitem_data = list(lineitem.find({
    "L_SHIPDATE": {
        "$gte": start_date,
        "$lt": end_date
    }
}, {
    "L_SUPPKEY": 1,
    "L_EXTENDEDPRICE": 1,
    "L_DISCOUNT": 1
}))

# Group by L_SUPPKEY and calculate total revenue
revenue_per_supplier = {}
for item in lineitem_data:
    supplier_no = item['L_SUPPKEY']
    revenue = calculate_revenue([item])
    if supplier_no in revenue_per_supplier:
        revenue_per_supplier[supplier_no] += revenue
    else:
        revenue_per_supplier[supplier_no] = revenue

# Find max revenue
max_revenue = max(revenue_per_supplier.values())

# Find suppliers with max revenue
suppliers_with_max_revenue = {k: v for k, v in revenue_per_supplier.items() if v == max_revenue}

# Fetch corresponding supplier details
supplier_data = list(supplier.find({
    "S_SUPPKEY": {
        "$in": list(suppliers_with_max_revenue.keys())
    }
}))

# Format data for CSV output
output_data = [{
    "S_SUPPKEY": s_data["S_SUPPKEY"],
    "S_NAME": s_data["S_NAME"],
    "S_ADDRESS": s_data["S_ADDRESS"],
    "S_PHONE": s_data["S_PHONE"],
    "TOTAL_REVENUE": suppliers_with_max_revenue[s_data["S_SUPPKEY"]]
} for s_data in supplier_data]

# Sort data by S_SUPPKEY
output_data_sorted = sorted(output_data, key=lambda s: s["S_SUPPKEY"])

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_PHONE", "TOTAL_REVENUE"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for row in output_data_sorted:
        writer.writerow(row)
