#!/bin/bash

# Update and install system-wide packages
apt-get update && apt-get install -y python3 python3-pip

# Install required Python packages
pip3 install pymongo==3.12.3 pymysql==1.0.2 pandas==1.4.1

# Create a directory for DirectRedis and clone the repo
mkdir -p /opt/direct_redis
cd /opt/direct_redis
git clone https://github.com/RedisGears/EdgeRealtimeVideoAnalytics.git .

# Install DirectRedis through its setup.py
cd EdgeRealtimeVideoAnalytics/direct_redis
python3 setup.py install
