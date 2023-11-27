#!/bin/bash

# Update repositories and Upgrade system
sudo apt-get update && sudo apt-get upgrade -y

# Install Python and pip
sudo apt-get install python3 python3-pip -y

# Install required Python packages
pip3 install pymysql pandas redis direct_redis
