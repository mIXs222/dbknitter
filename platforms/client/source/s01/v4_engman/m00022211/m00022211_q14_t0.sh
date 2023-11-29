#!/bin/bash

# Update package list
apt-get update

# Install python3 and pip if not already installed
apt-get install -y python3 python3-pip

# Install required python packages
pip3 install pymysql pymongo
