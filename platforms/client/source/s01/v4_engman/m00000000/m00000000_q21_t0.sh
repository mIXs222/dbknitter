# install_dependencies.sh
#!/bin/bash

# Update the package manager
sudo apt update

# Upgrade any existing packages
sudo apt upgrade -y

# Install Python 3 and pip if they are not installed
sudo apt install -y python3 python3-pip

# Install the PyMySQL library
pip3 install pymysql
