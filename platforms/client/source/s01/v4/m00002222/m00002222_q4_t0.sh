#!/bin/bash

# Run the script as root to install the dependencies
if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

# Make sure pip is installed
apt-get update
apt-get install -y python3-pip

# Install the required Python packages
pip3 install pandas
pip3 install direct-redis

# Note: Depending on the user's exact Redis data fetch implementation,
# additional dependencies for direct-redis might be required.
