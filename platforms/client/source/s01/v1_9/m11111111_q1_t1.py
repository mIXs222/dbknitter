from pymongo import MongoClient
import pandas as pd
import datetime

# setup connection
client = MongoClient('mongodb', 27017)
db = client["tpch"]
collection = db["lineitem"]

# define pipeline for aggregate query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$lte': datetime.datetime(1998, 9, 2)}
        }
    },
    {
        '$group': {
            '_id': {
                'L_RETURNFLAG': '$L_RETURNFLAG',
                'L_LINESTATUS': '$L_LINESTATUS'
            },
            'SUM_QTY': {'$sum': '$L_QUANTITY'},
            'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'SUM_DISC_PRICE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}] }},
            'SUM_CHARGE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}, {'$add': [1, '$L_TAX']}] }},
            'AVG_QTY': {'$avg': '$L_QUANTITY'},
            'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
            'AVG_DISC': {'$avg': '$L_DISCOUNT'},
            'COUNT_ORDER': {'$sum': 1}
        }
    },
    {
        '$sort': {
            '_id.L_RETURNFLAG': 1,
            '_id.L_LINESTATUS': 1
        }
    }
]

# execute the aggregate query
result = collection.aggregate(pipeline)

# converting the cursor into a list
result = list(result)

# preparing data for DataFrame
for doc in result:
    doc.update(doc.pop('_id'))

# create DataFrame
df = pd.DataFrame(result)

# write to CSV file
df.to_csv('query_output.csv', index = False)
