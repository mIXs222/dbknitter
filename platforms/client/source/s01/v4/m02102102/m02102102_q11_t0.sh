#!/bin/bash

# Install Python 3 and pip if not already installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install pymysql, pandas and direct_redis
pip3 install pymysql pandas direct_redis
