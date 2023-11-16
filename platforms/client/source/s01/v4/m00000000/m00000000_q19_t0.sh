#!/bin/bash
# install_dependencies.sh

# Update the package list
apt-get update

# Install Python and pip
apt-get install -y python3 python3-pip

# Install the pymysql package
pip3 install pymysql
