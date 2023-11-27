#!/bin/bash

# Update and Upgrade
apt-get update -y
apt-get upgrade -y

# Install Python and Python dependency management tool (pip)
apt-get install python3 -y
apt-get install python3-pip -y

# Install Python libraries
pip3 install pymysql
pip3 install pymongo
pip3 install pandas
pip3 install direct-redis
