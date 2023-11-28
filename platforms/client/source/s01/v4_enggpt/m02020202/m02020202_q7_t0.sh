# bash script

#!/bin/bash

# Update the system
apt-get update -y

# Install Python and pip
apt-get install python3 -y
apt-get install python3-pip -y

# Install the required Python libraries
pip3 install pymysql
pip3 install pandas
pip3 install direct_redis

# Run the Python script
python3 script.py
