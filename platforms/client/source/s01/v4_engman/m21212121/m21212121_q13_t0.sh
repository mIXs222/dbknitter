#!/bin/bash
# Bash script to install all dependencies (install_dependencies.sh)

# Update repositories and packages
sudo apt-get update
sudo apt-get upgrade -y

# Install Python and Pip
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install the necessary Python libraries
pip3 install pymongo
pip3 install direct-redis
pip3 install pandas
