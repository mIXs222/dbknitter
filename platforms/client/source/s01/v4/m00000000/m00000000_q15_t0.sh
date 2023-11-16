# install.sh
#!/bin/bash

# Update and upgrade package lists
apt-get update -y
apt-get upgrade -y

# Install pip for Python 3
apt-get install -y python3-pip

# Install PyMySQL
pip3 install pymysql
