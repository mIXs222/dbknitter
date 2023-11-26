# install_dependencies.sh
#!/bin/bash

# Update package list
apt-get update

# Install python3 and python3-pip if they are not installed
apt-get install -y python3 python3-pip

# Install Python libraries
pip3 install pandas pymysql direct-redis

# Run the Python script (if needed, uncomment the following line)
# python3 query.py
