# install_dependencies.sh
#!/bin/bash

# Update package list
sudo apt-get update

# Install Python3 and pip if not already installed
sudo apt-get install -y python3 python3-pip

# Install the pymysql library
pip3 install pymysql
