# setup.sh
#!/bin/bash
set -e

# Update package list and install pip if not available
sudo apt-get update
sudo apt-get install -y python3-pip

# Install required Python packages
pip3 install pymysql pymongo pandas
