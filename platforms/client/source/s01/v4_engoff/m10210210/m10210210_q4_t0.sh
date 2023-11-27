#!/bin/bash

# Update and install necessary packages
apt-get update
apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
