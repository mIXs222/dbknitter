# install_dependencies.sh
#!/bin/bash

# Update package list on your machine
apt-get update -y

# Install pip and Python development libraries if not already installed
apt-get install -y python3-pip python3-dev

# Install MongoDB and pymysql with pip
pip3 install pymongo pymysql
