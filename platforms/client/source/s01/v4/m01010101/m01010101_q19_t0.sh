#!/bin/bash

# Update the package list
apt-get -y update

# Install Python3 and pip
apt-get -y install python3 python3-pip

# Install the pymysql and pymongo libraries
pip3 install pymysql pymongo
