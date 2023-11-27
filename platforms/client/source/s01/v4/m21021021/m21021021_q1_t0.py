from pymongo import MongoClient
import csv

def connect_to_mongodb():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    return db

def execute_mongodb_query(db):
    pipeline = [
        {
            "$match": {
                "L_SHIPDATE": {
                    "$lte": "1998-09-02"
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "L_RETURNFLAG": "$L_RETURNFLAG",
                    "L_LINESTATUS": "$L_LINESTATUS"
                },
                "SUM_QTY": {
                    "$sum": "$L_QUANTITY"
                },
                "SUM_BASE_PRICE": {
                    "$sum": "$L_EXTENDEDPRICE"
                },
                "SUM_DISC_PRICE": {
                    "$sum": {
                        "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
                    }
                },
                "SUM_CHARGE": {
                    "$sum": {
                        "$multiply": [
                            {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]},
                            {"$add": [1, "$L_TAX"]}
                        ]
                    }
                },
                "AVG_QTY": { "$avg": "$L_QUANTITY" },
                "AVG_PRICE": { "$avg": "$L_EXTENDEDPRICE" },
                "AVG_DISC": { "$avg": "$L_DISCOUNT" },
                "COUNT_ORDER": { "$sum": 1 }
            }
        },
        {
            "$sort": {
                "_id.L_RETURNFLAG": 1,
                "_id.L_LINESTATUS": 1
            }
        }
    ]
    
    return list(db.lineitem.aggregate(pipeline))

def write_to_csv(data):
    with open('query_output.csv', 'w', newline='') as csvfile:
        fieldnames = ['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for entry in data:
            writer.writerow({
                'L_RETURNFLAG': entry['_id']['L_RETURNFLAG'],
                'L_LINESTATUS': entry['_id']['L_LINESTATUS'],
                'SUM_QTY': entry['SUM_QTY'],
                'SUM_BASE_PRICE': entry['SUM_BASE_PRICE'],
                'SUM_DISC_PRICE': entry['SUM_DISC_PRICE'],
                'SUM_CHARGE': entry['SUM_CHARGE'],
                'AVG_QTY': entry['AVG_QTY'],
                'AVG_PRICE': entry['AVG_PRICE'],
                'AVG_DISC': entry['AVG_DISC'],
                'COUNT_ORDER': entry['COUNT_ORDER']
            })

def main():
    db = connect_to_mongodb()
    mongodb_results = execute_mongodb_query(db)
    write_to_csv(mongodb_results)

if __name__ == "__main__":
    main()
