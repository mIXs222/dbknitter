# install_dependencies.sh
#!/bin/bash

# Update package lists
apt-get update

# Install pip and Python development files, if not already installed
apt-get install -y python3-pip python3-dev

# Install Python libraries required for connecting to MySQL, MongoDB, and Redis
pip3 install pymysql pymongo pandas direct_redis
