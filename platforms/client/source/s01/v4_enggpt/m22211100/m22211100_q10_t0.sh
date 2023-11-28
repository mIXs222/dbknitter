#!/bin/bash

sudo apt update

# Install Python 3 and pip
sudo apt install python3
sudo apt install python3-pip

# Install required Python libraries
pip3 install pymysql pymongo pandas direct-redis
