#!/bin/bash

# Python and PIP Installation
sudo apt update
sudo apt install -y python3 python3-pip

# Install the required Python packages
pip3 install pandas pymysql pymongo redis direct-redis
