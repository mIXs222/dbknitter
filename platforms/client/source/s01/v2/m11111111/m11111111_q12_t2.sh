#!/bin/sh
# make sure Python is installed
sudo apt-get update
sudo apt-get install python3.6
# make sure pip is installed
sudo apt-get install python3-pip
# install needed python libraries
pip3 install pymongo pandas
# run the script
python3 query_script.py
