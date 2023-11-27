# setup.sh
#!/bin/bash

# Update the package manager
apt-get update -y

# Install Python and pip
apt-get install python3 python3-pip -y

# Install pymysql and pandas
pip3 install pymysql pandas

# Install direct_redis
pip3 install git+https://github.com/paksu/py-direct-redis
