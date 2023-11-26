# install_dependencies.sh
#!/bin/bash

# Update package list
sudo apt-get update

# Install Python if not already installed (you can skip this step if Python is already installed)
sudo apt-get install -y python3

# Install pymysql
pip3 install pymysql
