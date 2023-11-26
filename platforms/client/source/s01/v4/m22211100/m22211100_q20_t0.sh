# install_dependencies.sh
#!/bin/bash

# Update repositories and install Python and pip
sudo apt update
sudo apt install -y python3 python3-pip 

# Install necessary Python packages
pip3 install pymysql pymongo pandas direct-redis
