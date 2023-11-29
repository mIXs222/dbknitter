#!/bin/bash

# Ensure pip is available
sudo apt-get install -y python3-pip

# Install pymongo
pip3 install pymongo

# Install pandas
pip3 install pandas

# Install redis-py
pip3 install redis

# Install direct_redis
pip3 install direct_redis

# Install necessary dependencies to handle datetime and timedelta
pip3 install python-dateutil pytz
