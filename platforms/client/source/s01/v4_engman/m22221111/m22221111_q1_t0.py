from pymongo import MongoClient
import pandas as pd
import csv
from datetime import datetime

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Construct and execute the query
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {
                "$lt": datetime(1998, 9, 2)
            }
        }
    },
    {
        "$group": {
            "_id": {
                "RETURNFLAG": "$L_RETURNFLAG",
                "LINESTATUS": "$L_LINESTATUS"
            },
            "SUM_QTY": {"$sum": "$L_QUANTITY"},
            "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
            "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] }},
            "SUM_CHARGE": {"$sum": {
                "$multiply": [
                    {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]},
                    {"$add": [1, "$L_TAX"]}
                ]
            }},
            "AVG_QTY": {"$avg": "$L_QUANTITY"},
            "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
            "AVG_DISC": {"$avg": "$L_DISCOUNT"},
            "COUNT_ORDER": {"$sum": 1}
        }
    },
    {
        "$sort": {
            "_id.RETURNFLAG": 1,
            "_id.LINESTATUS": 1
        }
    }
]

result = lineitem_collection.aggregate(pipeline)

# Convert the result to a pandas DataFrame
df = pd.DataFrame(result)
df = df.rename(columns={
    "_id": "STATUS",
    "SUM_QTY": "sum_qty",
    "SUM_BASE_PRICE": "sum_base_price",
    "SUM_DISC_PRICE": "sum_disc_price",
    "SUM_CHARGE": "sum_charge",
    "AVG_QTY": "avg_qty",
    "AVG_PRICE": "avg_price",
    "AVG_DISC": "avg_disc",
    "COUNT_ORDER": "count_order"
})

# Split STATUS column into RETURNFLAG and LINESTATUS
df['RETURNFLAG'] = df['STATUS'].apply(lambda x: x['RETURNFLAG'])
df['LINESTATUS'] = df['STATUS'].apply(lambda x: x['LINESTATUS'])
df.drop('STATUS', axis=1, inplace=True)

# Reordering columns to match the expected output
df = df[[
    'RETURNFLAG',
    'LINESTATUS',
    'sum_qty',
    'sum_base_price',
    'sum_disc_price',
    'sum_charge',
    'avg_qty',
    'avg_price',
    'avg_disc',
    'count_order'
]]

# Write the DataFrame to a csv file
df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
