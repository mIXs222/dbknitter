#!/bin/bash

# Update package list
apt-get update

# Install python3 and pip if they are not already installed
apt-get install -y python3 python3-pip

# Install direct_redis, pandas, pymysql, pymongo
pip3 install direct_redis pandas pymysql pymongo
