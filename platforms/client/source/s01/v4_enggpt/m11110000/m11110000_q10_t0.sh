#!/bin/bash

# Update and install necessary packages
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas
