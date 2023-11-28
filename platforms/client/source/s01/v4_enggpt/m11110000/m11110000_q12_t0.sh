#!/bin/bash
# install_dependencies.sh

# Update the repository information
apt-get update -y

# Install Python, Python's package manager (pip), and MySQL client
apt-get install -y python3 python3-pip default-mysql-client

# Install PyMySQL (Python MySQL client library)
pip3 install pymysql
