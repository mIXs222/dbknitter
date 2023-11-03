from pymongo import MongoClient
from decimal import Decimal
import csv

client = MongoClient("mongodb://mongodb:27017/")
db = client.tpch
lineitem = db.lineitem

pipeline = [
    {'$match':
         {'$and': [
             {'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'}},
             {'L_DISCOUNT': {'$gte': Decimal('.06') - Decimal('0.01'), '$lte': Decimal('.06') + Decimal('0.01')}},
             {'L_QUANTITY': {'$lt': 24}}
         ]}
     },
    {'$group':
         {'_id': None, 
          'REVENUE': {'$sum': {'$multiply': ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]}}}
    },
    {'$project':
         {'_id': 0, 'REVENUE': 1}
    }
]

output = list(lineitem.aggregate(pipeline))

with open(r'query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    for row in output:
        writer.writerow([row['REVENUE']])
