# file: install_dependencies.sh

#!/bin/bash
set -e

# Update package lists
apt-get update

# Install Python 3 and pip
apt-get install -y python3 python3-pip

# Install MySQL dependencies
apt-get install -y default-libmysqlclient-dev 

# Install redis-tools for direct_redis
apt-get install -y redis-tools

# Install Python packages
pip3 install pymysql pymongo pandas direct-redis
