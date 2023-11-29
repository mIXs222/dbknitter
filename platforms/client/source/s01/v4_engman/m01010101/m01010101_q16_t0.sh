# file: install_dependencies.sh
#!/bin/bash

# Update repository and install pip if not present
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymysql pymongo
