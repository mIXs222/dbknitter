from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client.tpch

# Function to apply discount and calculate revenue
def calculate_revenue(price, discount):
    return price * (1 - discount)

# Function to filter orders within the desired date range
def is_within_date_range(date_str, start_date, end_date):
    date_format = '%Y-%m-%d'
    order_date = datetime.strptime(date_str, date_format)
    return start_date <= order_date <= end_date

# Define date range
start_date = datetime.strptime('1990-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1994-12-31', '%Y-%m-%d')

# Query data from MongoDB and filter based on conditions
asia_nations = db.nation.aggregate([
    {'$lookup': {
        'from': 'region',
        'localField': 'N_REGIONKEY',
        'foreignField': 'R_REGIONKEY',
        'as': 'region'
    }},
    {'$match': {'region.R_NAME': 'ASIA'}}
])

asia_nation_keys = [nation['N_NATIONKEY'] for nation in asia_nations]

customers = db.customer.find({'C_NATIONKEY': {'$in': asia_nation_keys}}, {'_id': 0, 'C_CUSTKEY': 1, 'C_NATIONKEY': 1})
orders = db.orders.find({'O_CUSTKEY': {'$in': [c['C_CUSTKEY'] for c in customers]}}, {'_id': 0, 'O_ORDERKEY': 1, 'O_CUSTKEY': 1, 'O_ORDERDATE': 1})
lineitems = db.lineitem.find({'L_ORDERKEY': {'$in': [o['O_ORDERKEY'] for o in orders]}}, {'_id': 0, 'L_ORDERKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1})

# Filter orders by date and calculate revenue
revenue_by_nation = {}
for order in orders:
    if is_within_date_range(order['O_ORDERDATE'], start_date, end_date):
        for lineitem in lineitems:
            if lineitem['L_ORDERKEY'] == order['O_ORDERKEY']:
                revenue = calculate_revenue(lineitem['L_EXTENDEDPRICE'], lineitem['L_DISCOUNT'])
                customer = next((c for c in customers if c['C_CUSTKEY'] == order['O_CUSTKEY']), None)
                if customer:
                    nation_key = customer['C_NATIONKEY']
                    nation_name = db.nation.find_one({'N_NATIONKEY': nation_key}, {'_id': 0, 'N_NAME': 1})['N_NAME']
                    if nation_name not in revenue_by_nation:
                        revenue_by_nation[nation_name] = 0
                    revenue_by_nation[nation_name] += revenue

# Sort results by total revenue in descending order
sorted_revenues = sorted(revenue_by_nation.items(), key=lambda item: item[1], reverse=True)

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['NATION', 'TOTAL_REVENUE'])
    for nation_name, total_revenue in sorted_revenues:
        writer.writerow([nation_name, total_revenue])
