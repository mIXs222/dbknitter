# Bash script to install all dependencies

#!/bin/bash

# Activate virtual environment if you have one
# source /path_to_virtualenv/bin/activate

# Update the list of available packages and their versions
sudo apt-get update -y

# Install pip if it's not installed
sudo apt-get install -y python3-pip

# Install Python packages
pip3 install pymongo pandas direct_redis

# Note: You might need additional system-level dependencies depending on your system.
