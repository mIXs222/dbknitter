# install_dependencies.sh
#!/bin/bash

# Update package list and install python3 and pip if they are not installed
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the required Python libraries
pip3 install pymysql pandas direct_redis

# Run the Python script (make sure it's executable or use python3 to run it)
# python3 query_code.py
