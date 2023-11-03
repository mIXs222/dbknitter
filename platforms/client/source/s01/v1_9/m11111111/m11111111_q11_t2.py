from pymongo import MongoClient
import pandas as pd
import csv


def main():
    client = MongoClient("mongodb://mongodb:27017/")
    db = client.tpch

    german_suppliers = db.supplier.find({"S_NATIONKEY": db.nation.find_one({"N_NAME": 'GERMANY'})["N_NATIONKEY"]})
    german_supplier_keys = [supplier['S_SUPPKEY'] for supplier in german_suppliers]

    part_supp_german = list(db.partsupp.find({"PS_SUPPKEY": {"$in": german_supplier_keys}}))
    total_value = sum([part['PS_SUPPLYCOST'] * part['PS_AVAILQTY'] for part in part_supp_german]) * 0.0001000000

    result = []
    for part in part_supp_german:
        value = part['PS_SUPPLYCOST'] * part['PS_AVAILQTY']
        if value > total_value:
            result.append({"PS_PARTKEY": part['PS_PARTKEY'], "VALUE": value})

    result_sorted = sorted(result, key=lambda i: i['VALUE'], reverse=True)

    with open("query_output.csv", "w") as output:
        writer = csv.DictWriter(output, fieldnames=["PS_PARTKEY", "VALUE"])
        writer.writeheader()
        writer.writerows(result_sorted)


if __name__ == "__main__":
    main()
