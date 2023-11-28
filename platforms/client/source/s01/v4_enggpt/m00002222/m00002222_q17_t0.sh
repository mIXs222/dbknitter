#!/bin/bash

# Update the package list and upgrade packages
apt-get update
apt-get upgrade -y

# Install python3 and pip3 if they are not already installed
apt-get install python3 -y
apt-get install python3-pip -y

# Install Python libraries required by the code
pip3 install pymysql pandas redis direct_redis
