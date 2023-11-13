#!/bin/bash

echo "Updating packages"
sudo apt update -y

echo "Installing python3"
sudo apt install python3.8 -y

echo "Installing pip"
sudo apt install python3-pip -y

echo "Installing MongoDB's Driver- PyMongo"
python3 -m pip install pymongo

echo "Installing MySQL Connector for Python"
python3 -m pip install mysql-connector-python

echo "Installing Pandas"
python3 -m pip install pandas
