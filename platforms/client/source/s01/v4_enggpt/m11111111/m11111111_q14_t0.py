# mongo_query.py
from pymongo import MongoClient
from datetime import datetime
import csv

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# Define the date range
start_date = datetime.strptime('1995-09-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-09-30', '%Y-%m-%d')

# Query MongoDB
lineitem_query = db.lineitem.find({
    'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}
})

# Initialize totals
promo_revenue = 0
total_revenue = 0

# Process query results
for lineitem in lineitem_query:
    part = db.part.find_one({'P_PARTKEY': lineitem['L_PARTKEY']})
    if part and part['P_TYPE'].startswith('PROMO'):
        promo_revenue += lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])

    total_revenue += lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])

# Calculate percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue > 0 else 0

# Write to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promotional Revenue Percentage'])
    writer.writerow([promo_revenue_percentage])

# Close MongoDB connection
client.close()
