from pymongo import MongoClient
import pandas as pd
from datetime import datetime 

# Establish MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']
lineitem = db['lineitem']

# Define the query in MongoDB query language
pipeline = [
    {'$match': {
        'L_SHIPDATE': {
            '$lte': datetime.strptime('1998-09-02', '%Y-%m-%d')
        }
    }},
    {'$group': {
        '_id': {
            'L_RETURNFLAG': '$L_RETURNFLAG',
            'L_LINESTATUS': '$L_LINESTATUS'
        },
        'SUM_QTY': {'$sum': '$L_QUANTITY'},
        'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
        'SUM_DISC_PRICE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}},
        'SUM_CHARGE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}, {'$add': [1, '$L_TAX']}]}},
        'AVG_QTY': {'$avg': '$L_QUANTITY'},
        'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
        'AVG_DISC': {'$avg': '$L_DISCOUNT'},
        'COUNT_ORDER': {'$sum': 1}
    }},
    {'$sort': {
        '_id.L_RETURNFLAG': 1,
        '_id.L_LINESTATUS': 1
    }}
]

# Execute the query
response = lineitem.aggregate(pipeline)

# Extract results from cursor and write to CSV
results = list(response)
df = pd.DataFrame(results)
df.to_csv('query_output.csv', index=False)
