#!/bin/bash

# Install Python
apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pandas
pip3 install mysql-connector-python
pip3 install pymongo
pip3 install sqlalchemy
