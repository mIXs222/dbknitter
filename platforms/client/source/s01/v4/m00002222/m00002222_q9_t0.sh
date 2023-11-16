#!/bin/bash

# Install Python and Pip if they are not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pymysql pandas direct_redis
