#!/bin/bash
sudo apt-get update
sudo apt-get install -y python3-pip
pip3 install pandas
pip3 install mysql-connector-python pymongo
python3 your_python_filename.py
