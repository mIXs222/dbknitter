#!/bin/bash

# Install Python 3 and Pip if they are not installed
sudo apt-get update
sudo apt-get install python3 python3-pip -y

# Install required Python libraries
pip3 install pymysql pandas direct_redis
