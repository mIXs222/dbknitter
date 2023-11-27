#!/bin/bash

echo "Updating Python pip"
python -m pip install --upgrade pip

echo "Installing MySQL connector for Python"
pip install mysql-connector-python

echo "Installing PyMongo"
pip install pymongo

echo "Installing pandas"
pip install pandas
