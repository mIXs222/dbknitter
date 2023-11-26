# install_dependencies.sh
#!/bin/bash

# Update and install system-wide dependencies
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pandas pymysql pymongo direct_redis
