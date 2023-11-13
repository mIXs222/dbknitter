# Bash script: install_dependencies.sh
#!/bin/bash

# Update Package List
sudo apt-get update -y

# Upgrade Packages
sudo apt-get upgrade -y

# Install Python3 and Pip
sudo apt-get install python3 -y
sudo apt-get install python3-pip -y

# Install MySQl Connector via Pip
pip3 install mysql-connector-python

# Run Python Script
python3 query.py
