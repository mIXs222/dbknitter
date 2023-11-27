# install.sh
#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and Python3-pip if not installed
sudo apt-get install -y python3 python3-pip

# Install pymysql using pip
pip3 install pymysql
