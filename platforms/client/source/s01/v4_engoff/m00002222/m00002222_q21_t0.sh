#!/bin/bash

# Update package list and upgrade existing packages
apt-get update -y
apt-get upgrade -y

# Install Python and pip
apt-get install python3 -y
apt-get install python3-pip -y

# Install required Python packages
pip3 install pymysql pandas direct-redis
