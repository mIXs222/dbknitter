#!/bin/bash

#first navigate to the directory where python code resides
cd /path/to/python/code

# Install the pandas, pymongo and MySQL connector 
sudo apt install python3-pip
pip install pandas pymongo mysql-connector-python

#run the python code
python3 your_python_script.py
