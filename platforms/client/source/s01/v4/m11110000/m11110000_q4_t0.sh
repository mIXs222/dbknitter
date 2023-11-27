# install_dependencies.sh
#!/bin/bash

# Update the package manager
apt-get update

# Install Python3 and pip if they aren't already installed
apt-get install -y python3
apt-get install -y python3-pip

# Install pymysql using pip
pip3 install pymysql
