from pymongo import MongoClient
import csv
from bson.codec_options import TypeCodec, TypeRegistry, CodecOptions
from datetime import datetime
from decimal import Decimal

# Define a codec for Decimal type
class DecimalCodec(TypeCodec):
    python_type = Decimal
    bson_type = 'decimal'

    def transform_python(self, value):
        return str(value)

    def transform_bson(self, value):
        return Decimal(value)

# Register the codec for use with Decimal type
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Query the MongoDB database
pipeline = [
    {'$match': {
        'L_SHIPDATE': {'$lte': datetime(1998, 9, 2)}
    }},
    {'$group': {
        '_id': {'L_RETURNFLAG': '$L_RETURNFLAG', 'L_LINESTATUS': '$L_LINESTATUS'},
        'SUM_QTY': {'$sum': '$L_QUANTITY'},
        'SUM_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
        'SUM_DISC_PRICE': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}] }},
        'SUM_CHARGE': {'$sum': { '$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}, {'$add': [1, '$L_TAX']}] }},
        'AVG_QTY': {'$avg': '$L_QUANTITY'},
        'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
        'AVG_DISC': {'$avg': '$L_DISCOUNT'},
        'COUNT_ORDER': {'$sum': 1}
    }},
    {'$sort': {'_id.L_RETURNFLAG': 1, '_id.L_LINESTATUS': 1}}
]

lineitem = db.get_collection('lineitem', codec_options=codec_options)
results = lineitem.aggregate(pipeline)

# Write the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # Write headers
    writer.writerow([
        'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 
        'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 
        'AVG_DISC', 'COUNT_ORDER'
    ])
    
    # Write rows
    for result in results:
        writer.writerow([
            result['_id']['L_RETURNFLAG'], result['_id']['L_LINESTATUS'],
            result['SUM_QTY'], result['SUM_BASE_PRICE'],
            result['SUM_DISC_PRICE'], result['SUM_CHARGE'],
            result['AVG_QTY'], result['AVG_PRICE'],
            result['AVG_DISC'], result['COUNT_ORDER']
        ])
