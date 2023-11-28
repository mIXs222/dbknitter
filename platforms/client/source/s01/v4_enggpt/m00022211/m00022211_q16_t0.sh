# install.sh
#!/bin/bash

# Update and install Python and pip
sudo apt-get update
sudo apt-get install -y python3 python3-pip

# Install the necessary Python packages
pip3 install pymysql pandas direct-redis
