from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://mongodb:27017/")

db = client["tpch"]

def find_avg_quantity():
    pipeline = [
        {
            '$group': {
                '_id': '$P_PARTKEY',
                'avgQuantity': { '$avg': '$L_QUANTITY' }
            }
        }
    ]

    avg_quantity = db.lineitem.aggregate(pipeline)
    return list(avg_quantity)

avg_quantity_list = find_avg_quantity()
avg_quantity_dict = {item['_id']: item['avgQuantity'] for item in avg_quantity_list}

def find_output():
    pipeline = [
        {
            '$lookup': {
                'from': 'part',
                'localField': 'L_PARTKEY',
                'foreignField': 'P_PARTKEY',
                'as': 'part'
            }
        },
        {
            '$match': {
                'part.P_BRAND': 'Brand#23',
                'part.P_CONTAINER': 'MED BAG',
                'L_QUANTITY': { '$lt': avg_quantity_dict['part.P_PARTKEY'] * 0.2 }
            }
        },
        {
            '$group': {
                '_id': None,
                'totalExtendedPrice': { '$sum': '$L_EXTENDEDPRICE' }
            }
        }
    ]

    result = db.lineitem.aggregate(pipeline)
    return list(result)

output_list = find_output()

avg_yearly = output_list[0]['totalExtendedPrice'] / 7.0

df = pd.DataFrame({'AVG_YEARLY': [avg_yearly]})
df.to_csv('query_output.csv', index=False)
