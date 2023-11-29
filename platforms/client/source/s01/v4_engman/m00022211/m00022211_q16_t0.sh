# install_dependencies.sh
#!/bin/bash

# Update package lists
apt-get update

# Install pip and Python development files
apt-get install -y python3-pip python3-dev

# Install redis-py, which is the dependency for direct_redis
pip3 install redis

# Install direct_redis using the specific git repository (as it's not available on PyPI)
pip3 install git+https://github.com/patrys/direct_redis.git

# Install pymysql for MySQL connections
pip3 install pymysql

# Install pandas for handling data manipulation
pip3 install pandas
