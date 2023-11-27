from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# Define the query
part_query = {
    'P_BRAND': 'Brand#23',
    'P_CONTAINER': 'MED BAG'
}

lineitem_pipeline = [
    {
        '$lookup': {
            'from': 'part',
            'localField': 'L_PARTKEY',
            'foreignField': 'P_PARTKEY',
            'as': 'part_info'
        }
    },
    {
        '$unwind': '$part_info'
    },
    {
        '$match': {
            'part_info.P_BRAND': 'Brand#23',
            'part_info.P_CONTAINER': 'MED BAG'
        }
    },
    {
        '$group': {
            '_id': None,
            'average_quantity': {'$avg': '$L_QUANTITY'}
        }
    }
]

# Execute the queries
average_result = db.lineitem.aggregate(lineitem_pipeline)
average_quantity = next(average_result, {}).get('average_quantity', 0)
threshold_quantity = average_quantity * 0.2

lineitem_revenue_loss_pipeline = [
    {
        '$lookup': {
            'from': 'part',
            'localField': 'L_PARTKEY',
            'foreignField': 'P_PARTKEY',
            'as': 'part_info'
        }
    },
    {
        '$unwind': '$part_info'
    },
    {
        '$match': {
            'L_QUANTITY': {'$lt': threshold_quantity},
            'part_info.P_BRAND': 'Brand#23',
            'part_info.P_CONTAINER': 'MED BAG'
        }
    },
    {
        '$group': {
            '_id': None,
            'total_revenue_loss': {'$sum': '$L_EXTENDEDPRICE'}
        }
    }
]

# Calculate the average yearly revenue loss
revenue_loss_result = db.lineitem.aggregate(lineitem_revenue_loss_pipeline)
revenue_loss = next(revenue_loss_result, {}).get('total_revenue_loss', 0)
years = 7
average_yearly_revenue_loss = revenue_loss / years

# Output the results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['average_yearly_revenue_loss']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerow({'average_yearly_revenue_loss': average_yearly_revenue_loss})

print(f"Average yearly revenue loss: {average_yearly_revenue_loss}")

# Close the MongoDB connection
client.close()
