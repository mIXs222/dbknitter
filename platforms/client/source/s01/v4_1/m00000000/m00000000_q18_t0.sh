#!/bin/bash

# Install Python 3 and pip
yes | sudo apt install python3
yes | sudo apt install python3-pip

# Install pymysql, pymongo and direct_redis
pip3 install pymysql pymongo direct_redis pandas
