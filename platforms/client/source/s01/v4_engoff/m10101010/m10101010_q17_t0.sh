#!/bin/bash
apt-get update
apt-get install -y python3 python3-pip

# Install the necessary Python libraries
pip3 install pymongo pymysql
