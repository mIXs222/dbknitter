#!/bin/bash

# Update packages and install python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct_redis
