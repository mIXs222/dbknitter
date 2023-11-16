#!/bin/bash

# Python
sudo apt-get install -y python3.8

# pip
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py

# pandas
pip3 install pandas

# numpy
pip3 install numpy

# pymongo
pip3 install pymongo

# Cleanup
rm get-pip.py
