#!/bin/bash

# Install Python and Pip if they are not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the necessary libraries
pip3 install pandas pymysql pymongo direct_redis
