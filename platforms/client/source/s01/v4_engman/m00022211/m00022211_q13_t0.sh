# Save this code in a file named `install_dependencies.sh`

#!/bin/bash
# Bash script to install dependencies for query_exec.py

# Update package list and install pip
sudo apt-get update
sudo apt-get install -y python3-pip

# Install Python dependencies
pip3 install pymongo pandas direct_redis
