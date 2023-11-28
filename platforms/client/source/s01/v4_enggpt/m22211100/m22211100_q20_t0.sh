# File: install_dependencies.sh
#!/usr/bin/env bash

# Update and install system-wide packages
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required python libraries
pip3 install pymysql pymongo pandas redis direct_redis
