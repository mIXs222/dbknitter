#!/bin/bash
sudo apt-get update
sudo apt-get install -y python3
sudo apt-get install -y python3-pip
pip3 install pandas
pip3 install mysql-connector-python
pip3 install pymongo
python3 query.py
