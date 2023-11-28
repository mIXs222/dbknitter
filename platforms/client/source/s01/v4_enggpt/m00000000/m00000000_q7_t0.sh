# install_dependencies.sh
#!/bin/bash

# Update package index
apt-get update

# Install Python (if not already installed)
apt-get install -y python3

# Install pip (if not already installed)
apt-get install -y python3-pip

# Install pymysql
pip3 install pymysql
