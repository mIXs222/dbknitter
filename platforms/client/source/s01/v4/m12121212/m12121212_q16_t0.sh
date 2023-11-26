# filename: install_dependencies.sh
#!/bin/bash

# Update package lists
sudo apt-get update

# Install Python3 and pip if they are not installed
sudo apt-get install -y python3 python3-pip

# Make sure pip is up-to-date
pip3 install --upgrade pip

# Install required Python packages
pip3 install pymongo==3.12.3
pip3 install direct_redis
pip3 install pandas
