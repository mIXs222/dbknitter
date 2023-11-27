# install_dependencies.sh

#!/bin/bash

# Update and install Python and pip if not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install required Python libraries
pip3 install pandas pymysql pymongo direct-redis

# Write the command to execute the python script if needed
# python3 python_code.py
