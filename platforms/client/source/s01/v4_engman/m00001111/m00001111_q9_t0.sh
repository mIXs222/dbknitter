#!/bin/bash

# Update system package index
sudo apt update

# Install Python and pip
sudo apt install -y python3 python3-pip

# Install necessary Python libraries
pip3 install pymysql pymongo pandas
