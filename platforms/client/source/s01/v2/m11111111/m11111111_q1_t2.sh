#!/bin/sh

# first ensure pip, the python package installer is installed
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py

# install necessary dependencies
pip install pymongo
pip install pandas
