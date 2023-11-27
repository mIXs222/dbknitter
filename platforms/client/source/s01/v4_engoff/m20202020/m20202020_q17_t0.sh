# install_dependencies.sh
#!/bin/bash

# Update the package index
sudo apt-get update

# Install pip and Python dev tools
sudo apt-get install -y python3-pip python3-dev

# Install Python's pandas package
pip3 install pandas

# Install PyMySQL
pip3 install pymysql

# Install direct_redis
pip3 install direct_redis
