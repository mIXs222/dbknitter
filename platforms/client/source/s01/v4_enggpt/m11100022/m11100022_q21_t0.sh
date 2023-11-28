#!/bin/bash

# Install Python and Pip if they are not installed
sudo apt-get update
sudo apt-get install python3 python3-pip -y

# Install Python libraries
pip3 install pymysql pymongo pandas direct_redis
