#!/bin/sh
# install_dependencies.sh

# Make sure that Python and pip are already installed

# Update package index
sudo apt-get update

# Install pymysql
pip install pymysql
