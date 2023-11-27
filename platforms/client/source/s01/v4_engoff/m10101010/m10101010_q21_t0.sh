#!/bin/bash

# Update the package list
apt-get update

# Install pip if it's not already installed
apt-get install -y python3-pip

# Install pymysql and pymongo Python libraries
pip3 install pymysql
pip3 install pymongo
