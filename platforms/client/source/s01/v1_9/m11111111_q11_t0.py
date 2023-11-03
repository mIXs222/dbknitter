from pymongo import MongoClient
import pandas as pd

# Create a client connection
client = MongoClient("mongodb://mongodb:27017/")

# Connect to the tpch database
db = client['tpch']

# Select the collections
partsupp = db['partsupp']
supplier = db['supplier']
nation = db['nation']

# Aggregate query equivalent
pipeline_main = [{
    "$lookup":
        {"from": "supplier",
         "localField": "PS_SUPPKEY",
         "foreignField": "S_SUPPKEY",
         "as": "jointable1"
         }
},
    {"$unwind": "$jointable1"},
    {
        "$lookup":
            {"from": "nation",
             "localField": "jointable1.S_NATIONKEY",
             "foreignField": "N_NATIONKEY",
             "as": "jointable2"
             }
    },
    {"$unwind": "$jointable2"},
    {"$match": {"jointable2.N_NAME": 'GERMANY'}},
    {"$group": {
        "_id": "$PS_PARTKEY",
        "VALUE": {
            "$sum": {
                "$multiply": ["$PS_SUPPLYCOST", "$PS_AVAILQTY"]
            }
        }
    }
    }
]

# Subquery equivalent
pipeline_sub = [{
    "$lookup":
        {"from": "supplier",
         "localField": "PS_SUPPKEY",
         "foreignField": "S_SUPPKEY",
         "as": "jointable1"
         }
},
    {"$unwind": "$jointable1"},
    {
        "$lookup":
            {"from": "nation",
             "localField": "jointable1.S_NATIONKEY",
             "foreignField": "N_NATIONKEY",
             "as": "jointable2"
             }
    },
    {"$unwind": "$jointable2"},
    {"$match": {"jointable2.N_NAME": 'GERMANY'}},
    {"$group": {
        "_id": None,
        "TOTAL_VALUE": {
            "$sum": {
                "$multiply": ["$PS_SUPPLYCOST", "$PS_AVAILQTY"]
            }
        }
    }
    }
]

main_result = list(partsupp.aggregate(pipeline_main))
sub_result = list(partsupp.aggregate(pipeline_sub))

# Calculation and comparison
sub_value = sub_result[0]['TOTAL_VALUE'] * 0.0001000000
result = [row for row in main_result if row['VALUE'] > sub_value]

# Sort the result
result = sorted(result, key=lambda x: x['VALUE'], reverse=True)

# Write the result to a DataFrame then save to csv
df = pd.DataFrame(result)
df.to_csv('query_output.csv')
