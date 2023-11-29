from pymongo import MongoClient
import datetime
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Define collections
parts_collection = mongodb['part']
lineitem_collection = mongodb['lineitem']

# Define date range
start_date = datetime.datetime(1995, 9, 1)
end_date = datetime.datetime(1995, 10, 1)

# Query the database
promotional_parts_revenue = 0
total_revenue = 0

# Process each lineitem that was shipped within the date range
for lineitem in lineitem_collection.find({'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}}):
    # Calculate revenue for this lineitem
    revenue = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
    total_revenue += revenue

    # Check if part is promotional
    part = parts_collection.find_one({'P_PARTKEY': lineitem['L_PARTKEY']})
    if part:
        promotional_parts_revenue += revenue

# Calculate percentage if total_revenue is not zero
percentage_promotional_revenue = (promotional_parts_revenue / total_revenue) * 100 if total_revenue else 0

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['percentage_promotional_revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'percentage_promotional_revenue': percentage_promotional_revenue})

# Close MongoDB connection
client.close()
