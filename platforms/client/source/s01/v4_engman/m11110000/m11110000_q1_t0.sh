# install_dependencies.sh
#!/bin/bash

#Updating package lists
sudo apt-get update

# Installing Python 3 and pip, if not already installed
sudo apt-get install -y python3
sudo apt-get install -y python3-pip

# Installing pymysql
pip3 install pymysql
