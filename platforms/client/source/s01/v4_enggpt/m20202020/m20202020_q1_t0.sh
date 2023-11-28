#!/bin/bash
# Bash script (install_dependencies.sh)

# Update package index
apt-get update

# Install python3, pip and MySQL server
apt-get install -y python3 python3-pip default-mysql-server

# Install python pymysql package
pip3 install pymysql
