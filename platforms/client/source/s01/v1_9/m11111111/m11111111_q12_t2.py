from pymongo import MongoClient
import pandas as pd
from datetime import datetime

# Connect to the MongoDB client
client = MongoClient('mongodb://localhost:27017/')

# Access the tpch database
db = client['tpch']

def mongoQuery():
    # Perform the query
    pipeline = [
    { 
        "$match": { 
            "l_commitdate": { "$lt": "$l_receiptdate"},
            "l_shipdate": { "$lt": "$l_commitdate"},
            "l_receiptdate": { "$gte": datetime(1994, 1, 1), "$lt": datetime(1995, 1, 1)},
            "l_shipmode": { "$in": ["MAIL", "SHIP"]},
            "o_orderkey": "$l_orderkey"
        }
    },
    {
        "$group": {
            "_id": "$l_shipmode",
            "high_line_count": {
                "$sum": {
                    "$cond": [
                        { "$in": ["$o_orderpriority", ["1-URGENT", "2-HIGH"]] },
                        1,
                        0
                    ]
                }
            },
            "low_line_count": {
                "$sum": {
                    "$cond": [
                        { "$nin": ["$o_orderpriority", ["1-URGENT", "2-HIGH"]] },
                        1,
                        0
                    ]
                }
            }
        }
    },
    { "$sort": { "_id": 1}}
    ]

    # Execute the query
    return list(db.orders.aggregate(pipeline))

def main():
    # Query data and put it into a dataframe
    df = pd.DataFrame(mongoQuery())

    # write dataframe to output csv file
    df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    main()
