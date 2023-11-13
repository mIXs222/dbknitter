#!/bin/bash
# Update to latest package lists
sudo apt-get update

# Install python3 and pip3
sudo apt-get install python3
sudo apt-get install python3-pip

# Install necessary python packages
pip3 install pandas
pip3 install pymongo
pip3 install mysql-connector-python
